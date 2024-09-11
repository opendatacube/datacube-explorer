import warnings
from enum import Enum
from textwrap import dedent
from typing import Set

import structlog
from datacube.drivers.postgres._schema import DATASET as ODC_DATASET
from geoalchemy2 import Geometry
from sqlalchemy import (
    DDL,
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
    bindparam,
    func,
    select,
    text,
)
from sqlalchemy import (
    Enum as SqlEnum,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ProgrammingError

_LOG = structlog.get_logger()

CUBEDASH_SCHEMA = "cubedash"
METADATA = MetaData(schema=CUBEDASH_SCHEMA)
# GRIDCELL_COL_SPEC = f"{CUBEDASH_SCHEMA}.gridcell"

DATASET_SPATIAL = Table(
    "dataset_spatial",
    METADATA,
    # Note that we deliberately don't foreign-key to datacube tables:
    # - We don't want to add an external dependency on datacube core
    #   (breaking, eg, product deletion scripts)
    # - they may be in a separate database.
    Column("id", postgres.UUID(as_uuid=True), primary_key=True, comment="Dataset ID"),
    Column(
        "dataset_type_ref",
        SmallInteger,
        comment="The ODC dataset_type id",
        nullable=False,
    ),
    Column("center_time", DateTime(timezone=True), nullable=False),
    # When was the dataset created?
    # Creation_time if it has one, otherwise datacube index time.
    Column("creation_time", DateTime(timezone=True), nullable=False),
    # Nullable: Some products have no region.
    Column("region_code", String, comment=""),
    # Size of this dataset in bytes, if the product includes it.
    Column("size_bytes", BigInteger),
    Column("footprint", Geometry(spatial_index=False)),
    # Default postgres naming conventions.
    Index(
        "dataset_spatial_dataset_type_ref_center_time_idx",
        "dataset_type_ref",
        "center_time",
    ),
    # Faster region pages. Could be removed if faster summary generation is desired...
    Index(
        "dataset_spatial_dataset_type_ref_region_code_idx",
        "dataset_type_ref",
        "region_code",
        postgresql_ops={"region_code": "text_pattern_ops"},
    ),
)


DATASET_SPATIAL.indexes.add(
    Index(
        "dataset_spatial_footprint_wrs86_idx",
        func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326),
        postgresql_using="gist",
    )
)
# An index matching the default Stac API Item search and its sort order.
_COLLECTION_ITEMS_INDEX = Index(
    "dataset_spatial_collection_items_all_idx",
    "dataset_type_ref",
    "center_time",
    "id",
    _table=DATASET_SPATIAL,
)
# An index matching the default return of '/stac/search' (ie, all collections.)
_ALL_COLLECTIONS_ORDER_INDEX = Index(
    "dataset_spatial_all_collections_order_all_idx",
    "center_time",
    "id",
    _table=DATASET_SPATIAL,
)

DATASET_SPATIAL.indexes.add(_COLLECTION_ITEMS_INDEX)
DATASET_SPATIAL.indexes.add(_ALL_COLLECTIONS_ORDER_INDEX)

# Note that we deliberately don't foreign-key to datacube tables:
# - We don't want to add an external dependency on datacube core
#   (breaking, eg, product deletion scripts)
# - they may be in a separate database.
PRODUCT = Table(
    "product",
    METADATA,
    Column("id", SmallInteger, primary_key=True),
    Column("name", String, unique=True, nullable=False),
    Column("dataset_count", Integer, nullable=False),
    Column(
        "last_refresh",
        DateTime(timezone=True),
        nullable=False,
        comment="Last refresh of this product's extents'",
    ),
    Column(
        "last_successful_summary",
        DateTime(timezone=True),
        nullable=True,
        comment="The `last_refresh` time that was current when summaries "
        "were last *fully* generated successfully.",
    ),
    Column("source_product_refs", postgres.ARRAY(SmallInteger)),
    Column("derived_product_refs", postgres.ARRAY(SmallInteger)),
    Column("time_earliest", DateTime(timezone=True)),
    Column("time_latest", DateTime(timezone=True)),
    # A flat key-value set of metadata fields that are the same ("fixed") on every dataset.
    # (Almost always includes platform, instrument values)
    Column("fixed_metadata", postgres.JSONB),
)
TIME_OVERVIEW = Table(
    "time_overview",
    METADATA,
    # Uniquely identified by three values:
    Column("product_ref", None, ForeignKey(PRODUCT.c.id)),
    Column(
        "period_type", SqlEnum("all", "year", "month", "day", name="overviewperiod")
    ),
    Column("start_day", Date),
    Column("dataset_count", Integer, nullable=False),
    # Time range (if there's at least one dataset)
    Column("time_earliest", DateTime(timezone=True)),
    Column("time_latest", DateTime(timezone=True)),
    Column(
        "timeline_period",
        SqlEnum("year", "month", "week", "day", name="timelineperiod"),
        nullable=False,
    ),
    Column(
        "timeline_dataset_start_days",
        postgres.ARRAY(DateTime(timezone=True)),
        nullable=False,
    ),
    Column("timeline_dataset_counts", postgres.ARRAY(Integer), nullable=False),
    Column("regions", postgres.ARRAY(String), nullable=False),
    Column("region_dataset_counts", postgres.ARRAY(Integer), nullable=False),
    # The most newly created dataset
    Column("newest_dataset_creation_time", DateTime(timezone=True)),
    # When this summary was generated
    Column(
        "generation_time",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
    Column(
        "product_refresh_time",
        DateTime(timezone=True),
        # This is nullable in migrated schemas, as the update time is unknown.
        # (Those environments could be made non-null once everything is known to be refreshed)
        nullable=False,
        comment="The 'last_refresh' timestamp of the product at the time of generation.",
    ),
    Column("footprint_count", Integer, nullable=False),
    # SRID is overridden via config.
    Column("footprint_geometry", Geometry(srid=-999, spatial_index=False)),
    Column("crses", postgres.ARRAY(String)),
    # Size of this dataset in bytes, if the product includes it.
    Column("size_bytes", BigInteger),
    PrimaryKeyConstraint("product_ref", "start_day", "period_type"),
    CheckConstraint(
        r"array_length(timeline_dataset_start_days, 1) = "
        r"array_length(timeline_dataset_counts, 1)",
        name="timeline_lengths_equal",
    ),
)

# An SQLAlchemy expression to read the configured SRID.
FOOTPRINT_SRID_EXPRESSION = func.Find_SRID(
    TIME_OVERVIEW.schema, TIME_OVERVIEW.name, "footprint_geometry"
)

# The geometry of each unique 'region' for a product.
REGION = Table(
    "region",
    METADATA,
    Column("dataset_type_ref", SmallInteger, nullable=False),
    Column("region_code", String, nullable=False),
    Column("count", Integer, nullable=False),
    Column(
        "generation_time",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
    Column("footprint", Geometry(srid=4326, spatial_index=False)),
    PrimaryKeyConstraint("dataset_type_ref", "region_code"),
)


_REF_TABLE_METADATA = MetaData(schema=CUBEDASH_SCHEMA)
# This is a materialised view of the postgis spatial_ref_sys for lookups.
# See creation of mv_spatial_ref_sys below.
SPATIAL_REF_SYS = Table(
    "mv_spatial_ref_sys",
    _REF_TABLE_METADATA,
    Column("srid", Integer, primary_key=True),
    Column("auth_name", String(255)),
    Column("auth_srid", Integer),
    Column("srtext", String(2048)),
    Column("proj4text", String(2048)),
)

SPATIAL_QUALITY_STATS = Table(
    "mv_dataset_spatial_quality",
    _REF_TABLE_METADATA,
    Column("dataset_type_ref", SmallInteger, primary_key=True),
    Column("count", Integer),
    Column("missing_footprint", Integer),
    Column("footprint_size", Integer),
    Column("footprint_stddev", Numeric),
    Column("missing_srid", Integer),
    Column("has_file_size", Integer),
    Column("has_region", Integer),
)


def has_schema(conn: Connection) -> bool:
    """
    Does the cubedash schema already exist?
    """
    return conn.dialect.has_schema(conn, CUBEDASH_SCHEMA)


def is_compatible_schema(
    conn: Connection, odc_table_name: str, generate: bool = False
) -> bool:
    """
    Do we have the latest schema changes?
    If generate: Is the schema complete enough to run generate/refresh commands?
    """
    is_latest = True

    if not pg_column_exists(
        conn, f"{CUBEDASH_SCHEMA}.product", "last_successful_summary"
    ):
        is_latest = False

    if generate:
        # Incremental update scanning requires the optional `update` column on ODC.
        return is_latest and pg_column_exists(conn, odc_table_name, "updated")

    return is_latest


class SchemaNotRefreshableError(Exception):
    """The schema is not set-up for running product refreshes"""

    ...


class PleaseRefresh(Enum):
    """
    What data should be refreshed/recomputed?
    """

    # Refresh the product extents.
    PRODUCTS = 2
    # Recreate all dataset extents in the spatial table
    DATASET_EXTENTS = 1


def update_schema(conn: Connection) -> Set[PleaseRefresh]:
    """
    Update the schema if needed.

    Returns what data should be resummarised.
    """
    # Will never return PleaseRefresh.PRODUCTS...

    refresh = set()

    if not pg_column_exists(conn, f"{CUBEDASH_SCHEMA}.product", "fixed_metadata"):
        _LOG.warning("schema.applying_update.add_fixed_metadata")
        conn.execute(
            text(
                f"alter table {CUBEDASH_SCHEMA}.product add column fixed_metadata jsonb"
            )
        )
        refresh.add(PleaseRefresh.DATASET_EXTENTS)

    # why are these in update_schema rather than create_schema?
    _COLLECTION_ITEMS_INDEX.create(conn, checkfirst=True)

    _ALL_COLLECTIONS_ORDER_INDEX.create(conn, checkfirst=True)

    pg_add_column(
        conn,
        CUBEDASH_SCHEMA,
        "time_overview",
        "product_refresh_time",
        "timestamp with time zone null",
    )

    pg_add_column(
        conn,
        CUBEDASH_SCHEMA,
        "product",
        "last_successful_summary",
        "timestamp with time zone null",
    )

    check_or_update_odc_schema(conn)

    return refresh


def check_or_update_odc_schema(conn: Connection):
    """
    Check that the ODC schema is updated enough to run Explorer,

    and either update it safely (if we have permission), or tell the user how.
    """
    # We need the `update` column on ODC's dataset table in order to run incremental product refreshes.
    try:
        # We can try to install it ourselves if we have permission, using ODC's code.
        if not pg_column_exists(conn, ODC_DATASET.fullname, "updated"):
            _LOG.warning("schema.applying_update.add_odc_change_triggers")
            from datacube.drivers.postgres._core import install_timestamp_trigger

            # shouldn't be a need to account for ImportError anymore
            install_timestamp_trigger(conn)
    except ProgrammingError as e:
        # We don't have permission.
        raise SchemaNotRefreshableError(
            dedent(
                """
            Missing update triggers.

            No dataset-update triggers are installed on the ODC instance, and Explorer does
            not have enough permissions to add them itself.

            It's recommended to run `datacube system init` on your ODC instance to install them.

            Then try this again.
        """
            )
        ) from e

    # Add optional indexes to AGDC if we have permission.
    # (otherwise we warn the user that it may be slow, and how to add it themselves)
    # statements = []
    try:
        pg_create_index(conn, "ix_dataset_added", ODC_DATASET.fullname, "added desc")
        pg_create_index(
            conn,
            "ix_dataset_type_changed",
            ODC_DATASET.fullname,
            "dataset_type_ref, greatest(added, updated, archived) desc",
        )
    except ProgrammingError:
        warnings.warn(
            dedent(
                """
                No recently-added index.
                Explorer recommends adding an index for recently-added datasets to your ODC,
                but does not have permission to add it to the current ODC database.
                """
            ),
            stacklevel=2,
        )
        raise


def pg_create_index(
    conn,
    idx_name: str,
    table_name: str,
    col_expr: str | None = None,
    unique: bool = False,
):
    conn.execute(
        text(
            f"create {'unique' if unique else ''} index if not exists {idx_name} on {table_name}({col_expr})"
        )
    )


def pg_index_exists(conn, schema_name: str, table_name: str, index_name: str) -> bool:
    """
    Does a postgres index exist?

    Unlike pg_exists(), we don't need heightened permissions on the table.

    So, for example, Explorer's limited-permission user can check agdc/ODC tables
    that it doesn't own.
    """
    return (
        conn.execute(
            text("""
                select indexname
                from pg_indexes
                where schemaname=:schema_name and
                    tablename=:table_name and
                    indexname=:index_name
              """),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "index_name": index_name,
            },
        ).scalar()
        is not None
    )


def get_postgis_versions(conn) -> str:
    """What versions of Postgis, Postgres and libs do we have?"""
    return conn.execute(select(func.postgis_full_version())).scalar()


def pg_add_column(
    conn, schema_name: str, table_name: str, column_name: str, column_type: str
):
    conn.execute(
        text(
            f"alter table {schema_name}.{table_name} add column if not exists {column_name} {column_type}"
        )
    )


def pg_column_exists(conn, table_name: str, column_name: str) -> bool:
    """
    Does a postgres column exist?
    """
    schema_name, table_name = table_name.split(".")
    return (
        conn.execute(
            text("""
                select 1
                from information_schema.columns
                where table_name = :table_name
                    and table_schema = :schema_name
                    and column_name = :column_name
            """),
            {
                "table_name": table_name,
                "schema_name": schema_name,
                "column_name": column_name,
            },
        ).scalar()
        is not None
    )


def _epsg_to_srid(conn: Connection, code: int) -> int:
    """
    Convert an epsg code to Postgis' srid number.

    They're usually the same in Postgis' default srid table... but they don't
    have to be. We'll do this lookup anyway to be good citizens.
    """
    return conn.execute(
        text(
            f"select srid from spatial_ref_sys where auth_name = 'EPSG' and auth_srid={code}"
        )
    ).scalar()


def create_schema(conn: Connection, epsg_code: int):
    """
    Create any missing parts of the cubedash schema
    """
    # Create schema if needed.
    #
    # Note that we don't use the built-in "if not exists" because running it *always* requires
    # `create` permission.
    #
    # Doing it separately allows users to run this tool without `create` permission.
    #
    if not has_schema(conn):
        conn.execute(DDL(f"create schema {CUBEDASH_SCHEMA}"))

    # Add Postgis if needed
    #
    # Note that, as above, we deliberately don't use the built-in "if not exists"
    #
    if (
        conn.execute(
            text("select count(*) from pg_extension where extname='postgis';")
        ).scalar()
        == 0
    ):
        conn.execute(DDL("create extension postgis"))

    srid = _epsg_to_srid(conn, epsg_code)
    if srid is None:
        raise RuntimeError(
            f"Postgis doesn't seem to know about epsg code {epsg_code!r}."
        )

    # Our global SRID.
    TIME_OVERVIEW.c.footprint_geometry.type.srid = srid

    # We want an index on the spatial_ref_sys table to do authority name/code lookups.
    # But in RDS environments we cannot add indexes to it.
    # So we create our own copy as a materialised view (it's a very small table).
    conn.execute(
        text(f"""
    create materialized view if not exists {CUBEDASH_SCHEMA}.mv_spatial_ref_sys
        as select * from spatial_ref_sys;
    """)
    )
    # The normal primary key.
    # conn.execute(
    #     text(f"""
    #     create unique index if not exists mv_spatial_ref_sys_srid_idx on
    #         {CUBEDASH_SCHEMA}.mv_spatial_ref_sys(srid);
    #     """)
    # )
    pg_create_index(
        conn,
        "mv_spatial_ref_sys_srid_idx",
        f"{CUBEDASH_SCHEMA}.mv_spatial_ref_sys",
        "srid",
        unique=True,
    )
    # For case insensitive auth name/code lookups.
    # (Postgis doesn't add one by default, but we're going to do a lot of lookups)
    # conn.execute(
    #     text(f"""
    #     create unique index if not exists mv_spatial_ref_sys_lower_auth_srid_idx on
    #         {CUBEDASH_SCHEMA}.mv_spatial_ref_sys(lower(auth_name::text), auth_srid);
    #     """)
    # )
    pg_create_index(
        conn,
        "mv_spatial_ref_sys_lower_auth_srid_idx",
        f"{CUBEDASH_SCHEMA}.mv_spatial_ref_sys",
        "lower(auth_name::text), auth_srid",
        unique=True,
    )

    METADATA.create_all(conn, checkfirst=True)

    # Useful reporting.
    # could move this out of create_schema, and then call it in index.api.init_schema
    conn.execute(
        text(f"""
    create materialized view if not exists {CUBEDASH_SCHEMA}.mv_dataset_spatial_quality as (
        select
            dataset_type_ref,
            count(*) as count,
            count(*) filter (where footprint is null) as missing_footprint,
            sum(pg_column_size(footprint)) filter (where footprint is not null) as footprint_size,
            stddev(pg_column_size(footprint)) filter (where footprint is not null) as footprint_stddev,
            count(*) filter (where ST_SRID(footprint) is null) as missing_srid,
            count(*) filter (where size_bytes is not null) as has_file_size,
            count(*) filter (where region_code is not null) as has_region
        from {CUBEDASH_SCHEMA}.dataset_spatial
        group by dataset_type_ref
    ) with no data;
    """)
    )

    conn.execute(
        text(f"""
    create unique index if not exists mv_dataset_spatial_quality_dataset_type_ref
        on {CUBEDASH_SCHEMA}.mv_dataset_spatial_quality(dataset_type_ref);
    """)
    )


def refresh_supporting_views(conn, concurrently=False):
    args = "concurrently" if concurrently else ""
    conn.execute(
        text(f"""
    refresh materialized view {args} {CUBEDASH_SCHEMA}.mv_spatial_ref_sys;
    """)
    )
    conn.execute(
        text(f"""
    refresh materialized view {args} {CUBEDASH_SCHEMA}.mv_dataset_spatial_quality;
    """)
    )


def get_srid_name(conn: Connection, srid: int):
    """
    Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
    """
    return conn.execute(
        select(
            func.concat(
                SPATIAL_REF_SYS.c.auth_name,
                ":",
                SPATIAL_REF_SYS.c.auth_srid.cast(Integer),
            )
        ).where(SPATIAL_REF_SYS.c.srid == bindparam("srid", srid, type_=Integer))
    ).scalar()


def init_elements(conn: Connection, grouping_epsg_code: int):
    """
    Initialise any schema elements that don't exist.

    Takes an epsg_code, of the CRS used internally for summaries.

    (Requires `create` permissions in the db)
    """
    srid = conn.execute(select(FOOTPRINT_SRID_EXPRESSION)).scalar()
    grouping_crs = get_srid_name(conn, srid)
    # Add any missing schema items or patches.
    create_schema(conn, epsg_code=grouping_epsg_code)

    # If they specified an epsg code, make sure the existing schema uses it.
    if grouping_epsg_code:
        crs_used_by_schema = grouping_crs
        if crs_used_by_schema != f"EPSG:{grouping_epsg_code}":
            raise RuntimeError(
                f"""
                Tried to initialise with EPSG:{grouping_epsg_code!r},
                but the schema is already using {crs_used_by_schema}.

                To change the CRS, you need to recreate Explorer's schema.

                Eg.

                    # Drop schema
                    cubedash-gen --drop

                    # Create schema with new epsg, and summarise all products again.
                    cubedash-gen --init --epsg {grouping_epsg_code} --all

                (Warning: Resummarising all of your products may take a long time!)
                """
            )
    return update_schema(conn)
