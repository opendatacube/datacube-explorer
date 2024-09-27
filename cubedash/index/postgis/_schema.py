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
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
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
from sqlalchemy.orm import registry

from cubedash.summary._schema import (
    CUBEDASH_SCHEMA,
    METADATA,
    REF_TABLE_METADATA,
    epsg_to_srid,
    has_schema,
    pg_create_index,
)

orm_registry = registry()


@orm_registry.mapped
class DatasetSpatial:
    __tablename__ = "dataset_spatial"
    __table_args__ = (
        METADATA,
        # Default postgres naming conventions.
        Index(
            "dataset_spatial_product_ref_center_time_idx", "product_ref", "center_time"
        ),
        # Faster region pages. Could be removed if faster summary generation is desired...
        Index(
            "dataset_spatial_product_ref_region_code_idx",
            "product_ref",
            "region_code",
            postgresql_ops={"region_code": "text_pattern_ops"},
        ),
        # An index matching the default Stac API Item search and its sort order.
        Index(
            "dataset_spatial_collection_items_all_idx",
            "product_ref",
            "center_time",
            "id",
        ),
        # An index matching the default return of '/stac/search' (ie, all collections.)
        Index("dataset_spatial_all_collections_order_all_idx", "center_time", "id"),
        {"schema": CUBEDASH_SCHEMA, "comment": "A dataset."},
    )
    # Note that we deliberately don't foreign-key to datacube tables:
    # - We don't want to add an external dependency on datacube core
    #   (breaking, eg, product deletion scripts)
    # - they may be in a separate database.
    id = Column(postgres.UUID(as_uuid=True), primary_key=True, comment="Dataset ID")
    product_ref = Column(SmallInteger, comment="The ODC product id", nullable=False)
    center_time = Column(DateTime(timezone=True), nullable=False)
    # When was the dataset created?
    # Creation_time if it has one, otherwise datacube index time.
    creation_time = Column(DateTime(timezone=True), nullable=False)
    # Nullable: Some products have no region.
    region_code = Column(String, comment="")
    # Size of this dataset in bytes, if the product includes it.
    size_bytes = Column(BigInteger)
    footprint = Column(Geometry(spatial_index=False))


Index(
    "dataset_spatial_footprint_wrs86_idx",
    func.ST_Transform(DatasetSpatial.footprint, 4326),
    postgresql_using="gist",
)


# Note that we deliberately don't foreign-key to datacube tables:
# - We don't want to add an external dependency on datacube core
#   (breaking, eg, product deletion scripts)
# - they may be in a separate database.
@orm_registry.mapped
class Product:
    __tablename__ = "product"
    __table_args__ = (METADATA, {"schema": CUBEDASH_SCHEMA, "comment": "A product."})
    id = Column(SmallInteger, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    dataset_count = Column(Integer, nullable=False)
    last_refresh = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="Last refresh of this product's extents'",
    )
    last_successful_summary = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="The `last_refresh` time that was current when summaries "
        "were last *fully* generated successfully.",
    )
    source_product_refs = Column(postgres.ARRAY(SmallInteger))
    derived_product_refs = Column(postgres.ARRAY(SmallInteger))
    time_earliest = Column(DateTime(timezone=True))
    time_latest = Column(DateTime(timezone=True))
    # A flat key-value set of metadata fields that are the same ("fixed") on every dataset.
    # (Almost always includes platform, instrument values)
    fixed_metadata = Column(postgres.JSONB)


@orm_registry.mapped
class TimeOverview:
    __tablename__ = "time_overview"
    __table_args__ = (
        METADATA,
        PrimaryKeyConstraint("product_ref", "start_day", "period_type"),
        CheckConstraint(
            r"array_length(timeline_dataset_start_days, 1) = "
            r"array_length(timeline_dataset_counts, 1)",
            name="timeline_lengths_equal",
        ),
        {
            "schema": CUBEDASH_SCHEMA,
        },
    )
    # Uniquely identified by three values:
    product_ref = Column(None, ForeignKey(Product.id))
    period_type = Column(SqlEnum("all", "year", "month", "day", name="overviewperiod"))
    start_day = Column(Date)
    dataset_count = Column(Integer, nullable=False)
    # Time range (if there's at least one dataset)
    time_earliest = Column(DateTime(timezone=True))
    time_latest = Column(DateTime(timezone=True))
    timeline_period = Column(
        SqlEnum("year", "month", "week", "day", name="timelineperiod"),
        nullable=False,
    )
    timeline_dataset_start_days = Column(
        postgres.ARRAY(DateTime(timezone=True)),
        nullable=False,
    )
    timeline_dataset_counts = Column(postgres.ARRAY(Integer), nullable=False)
    regions = Column(postgres.ARRAY(String), nullable=False)
    region_dataset_counts = Column(postgres.ARRAY(Integer), nullable=False)
    # The most newly created dataset
    newest_dataset_creation_time = Column(DateTime(timezone=True))
    # When this summary was generated
    generation_time = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    product_refresh_time = Column(
        DateTime(timezone=True),
        # This is nullable in migrated schemas, as the update time is unknown.
        # (Those environments could be made non-null once everything is known to be refreshed)
        nullable=False,
        comment="The 'last_refresh' timestamp of the product at the time of generation.",
    )
    footprint_count = Column(Integer, nullable=False)
    # SRID is overridden via config.
    footprint_geometry = Column(Geometry(srid=-999, spatial_index=False))
    crses = Column(postgres.ARRAY(String))
    # Size of this dataset in bytes, if the product includes it.
    size_bytes = Column(BigInteger)


# An SQLAlchemy expression to read the configured SRID.
FOOTPRINT_SRID_EXPRESSION = func.Find_SRID(
    # how to access 'schema' and 'name' from TimeOverview?
    CUBEDASH_SCHEMA,
    "time_overview",
    "footprint_geometry",  # should this be TimeOverview.footprint_geometry
)


@orm_registry.mapped
class Region:
    __tablename__ = "region"
    __table_args__ = (
        METADATA,
        PrimaryKeyConstraint("product_ref", "region_code"),
        {
            "schema": CUBEDASH_SCHEMA,
            "comment": "The geometry of each unique 'region' for a product.",
        },
    )
    product_ref = Column(SmallInteger, nullable=False)
    region_code = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    generation_time = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    footprint = Column(Geometry(srid=4326, spatial_index=False))


# This is a materialised view of the postgis spatial_ref_sys for lookups.
# See creation of mv_spatial_ref_sys below.
@orm_registry.mapped
class SpatialRefSys:
    __tablename__ = "mv_spatial_ref_sys"
    __table_args__ = (
        REF_TABLE_METADATA,
        {
            "schema": CUBEDASH_SCHEMA,
        },
    )
    srid = Column(Integer, primary_key=True)
    auth_name = Column(String(255))
    auth_srid = Column(Integer)
    srtext = Column(String(2048))
    proj4text = Column(String(2048))


@orm_registry.mapped
class SpatialQualityStats:
    __tablename__ = "mv_dataset_spatial_quality"
    __table_args__ = (
        REF_TABLE_METADATA,
        {
            "schema": CUBEDASH_SCHEMA,
        },
    )
    product_ref = Column(SmallInteger, primary_key=True)
    count = Column(Integer)
    missing_footprint = Column(Integer)
    footprint_size = Column(Integer)
    footprint_stddev = Column(Numeric)
    missing_srid = Column(Integer)
    has_file_size = Column(Integer)
    has_region = Column(Integer)


def get_srid_name(conn: Connection, srid: int):
    """
    Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
    """
    return conn.execute(
        select(
            func.concat(
                SpatialRefSys.auth_name,
                ":",
                SpatialRefSys.auth_srid.cast(Integer),  # do we really need the cast?
            )
        ).where(SpatialRefSys.srid == bindparam("srid", srid, type_=Integer))
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

    srid = epsg_to_srid(conn, epsg_code)
    if srid is None:
        raise RuntimeError(
            f"Postgis doesn't seem to know about epsg code {epsg_code!r}."
        )

    # Our global SRID.
    TimeOverview.footprint_geometry.type.srid = srid

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
    pg_create_index(
        conn,
        "mv_spatial_ref_sys_srid_idx",
        f"{CUBEDASH_SCHEMA}.mv_spatial_ref_sys",
        "srid",
        unique=True,
    )
    # For case insensitive auth name/code lookups.
    # (Postgis doesn't add one by default, but we're going to do a lot of lookups)
    pg_create_index(
        conn,
        "mv_spatial_ref_sys_lower_auth_srid_idx",
        f"{CUBEDASH_SCHEMA}.mv_spatial_ref_sys",
        "lower(auth_name::text), auth_srid",
        unique=True,
    )

    # is there a way to ensure orm_registry.metadata doesn't include the ref_table_metadata tables?
    non_ref_tables = [
        DatasetSpatial.__table__,
        Product.__table__,
        TimeOverview.__table__,
        Region.__table__,
    ]
    orm_registry.metadata.create_all(conn, tables=non_ref_tables, checkfirst=True)

    # Useful reporting.
    conn.execute(
        text(f"""
    create materialized view if not exists {CUBEDASH_SCHEMA}.mv_dataset_spatial_quality as (
        select
            product_ref,
            count(*) as count,
            count(*) filter (where footprint is null) as missing_footprint,
            sum(pg_column_size(footprint)) filter (where footprint is not null) as footprint_size,
            stddev(pg_column_size(footprint)) filter (where footprint is not null) as footprint_stddev,
            count(*) filter (where ST_SRID(footprint) is null) as missing_srid,
            count(*) filter (where size_bytes is not null) as has_file_size,
            count(*) filter (where region_code is not null) as has_region
        from {CUBEDASH_SCHEMA}.dataset_spatial
        group by product_ref
    ) with no data;
    """)
    )

    pg_create_index(
        conn,
        "mv_dataset_spatial_quality_product_ref",
        f"{CUBEDASH_SCHEMA}.mv_dataset_spatial_quality",
        "product_ref",
        unique=True,
    )


def init_elements(conn: Connection, grouping_epsg_code: int):
    """
    Initialise any schema elements that don't exist.

    Takes an epsg_code, of the CRS used internally for summaries.

    (Requires `create` permissions in the db)
    """
    # Add any missing schema items or patches.
    create_schema(conn, epsg_code=grouping_epsg_code)

    # If they specified an epsg code, make sure the existing schema uses it.
    srid = conn.execute(select(FOOTPRINT_SRID_EXPRESSION)).scalar()
    crs_used_by_schema = get_srid_name(conn, srid)
    # hopefully default epsg wouldn't case an issue?
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

    # no need to add potentially missing columns because we know postgis will have them

    return set()
