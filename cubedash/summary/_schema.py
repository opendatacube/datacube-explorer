import contextlib
from collections.abc import Generator
from enum import Enum
from typing import Literal

import structlog
from sqlalchemy import MetaData, func, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ProgrammingError

_LOG = structlog.stdlib.get_logger()

CUBEDASH_SCHEMA = "cubedash"
METADATA = MetaData(schema=CUBEDASH_SCHEMA)
REF_TABLE_METADATA = MetaData(schema=CUBEDASH_SCHEMA)


def is_compatible_schema(conn: Connection, odc_table_name: str, generate: bool) -> bool:
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
    conn, idx_name: str, table_name: str, col_expr: str | None, unique: bool = False
) -> None:
    conn.execute(
        text(
            f"create {'unique' if unique else ''} index if not exists {idx_name} on {table_name}({col_expr})"
        )
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


def epsg_to_srid(conn: Connection, code: int) -> int | None:
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


def refresh_supporting_views(conn, concurrently: bool) -> None:
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


def transfers_required(
    conn: Connection,
    new_owner: str,
    objects: list[str],
    object_type: Literal["tables", "matviews"],
) -> list[tuple[str, str]]:
    """
    Returns a list of (name, old_owner) tuples for objects that need to be transferred to the new owner.
    """
    transfers: list[tuple[str, str]] = []
    defs = {
        "tables": ("tablename", "tableowner", "pg_tables"),
        "matviews": ("matviewname", "matviewowner", "pg_matviews"),
    }
    n, o, t = defs[object_type]
    sql = f"select {n}, {o} from {t} where schemaname = '{CUBEDASH_SCHEMA}' and {n} in {tuple(objects)}"
    for row in conn.execute(text(sql)):
        if row[1] != new_owner:
            transfers.append((row[0], row[1]))
    return transfers


def transfer_owner(
    conn: Connection,
    obj_name: str,
    current_owner: str,
    new_owner: str,
    object_type: Literal["tables", "matviews"],
) -> None:
    objs = {
        "tables": "table",
        "matviews": "materialized view",
    }
    try:
        with as_role(conn, current_owner) as owner_conn:
            owner_conn.execute(
                text(
                    f"alter {objs[object_type]} {CUBEDASH_SCHEMA}.{obj_name} owner to {new_owner}"
                )
            )
    except ProgrammingError:
        _LOG.warning(
            f"Cannot transfer ownership of table {obj_name} from {current_owner} to {new_owner}: "
            f"active user is not a superuser or current owner"
        )


def roles_exist(conn: Connection, roles: list[str]) -> bool:
    return all(
        conn.execute(text(f"select 1 from pg_roles where rolname = '{role}'")).scalar()
        for role in roles
    )


@contextlib.contextmanager
def as_role(conn: Connection, role: str | None) -> Generator[Connection]:
    if role is None:
        yield conn
    else:
        db_user = conn.execute(text("select quote_ident(current_user)")).scalar()
        conn.execute(text(f"set role {role}"))
        yield conn
        conn.execute(text(f"set role {db_user}"))
