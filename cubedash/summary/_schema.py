from enum import Enum

import structlog
from sqlalchemy import (
    MetaData,
    func,
    select,
    text,
)
from sqlalchemy.engine import Connection

_LOG = structlog.get_logger()

CUBEDASH_SCHEMA = "cubedash"
METADATA = MetaData(schema=CUBEDASH_SCHEMA)
REF_TABLE_METADATA = MetaData(schema=CUBEDASH_SCHEMA)


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


def pg_create_index(
    conn,
    idx_name: str,
    table_name: str,
    col_expr: str | None = None,
    unique: bool = False,
) -> None:
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
) -> None:
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


def epsg_to_srid(conn: Connection, code: int) -> int:
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


def refresh_supporting_views(conn, concurrently=False) -> None:
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
