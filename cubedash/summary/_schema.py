from __future__ import absolute_import

import re

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Date, BigInteger, PrimaryKeyConstraint
from sqlalchemy import Enum, DDL, CheckConstraint
from sqlalchemy import func, Table, Column, ForeignKey, String, Integer, SmallInteger, MetaData, Index
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine

CUBEDASH_SCHEMA = 'cubedash'
METADATA = MetaData(schema=CUBEDASH_SCHEMA)
GRIDCELL_COL_SPEC = f'{CUBEDASH_SCHEMA}.gridcell'

# This is a materialised view of the postgis spatial_ref_sys for lookups. See creation of mv_spatial_ref_sys below.
SPATIAL_REF_SYS = Table(
    'mv_spatial_ref_sys', METADATA,
    Column('srid', Integer, primary_key=True),
    Column('auth_name', String(255)),
    Column('auth_srid', Integer),
    Column('srtext', String(2048)),
    Column('proj4text', String(2048)),
)

# Albers equal area. Allows us to show coverage in m^2 easily.
FOOTPRINT_SRID = 3577

DATASET_SPATIAL = Table(
    'dataset_spatial',
    METADATA,
    # Note that we deliberately don't foreign-key to datacube tables:
    # - We don't want to add an external dependency on datacube core (breaking, eg, product deletion scripts)
    # - they may be in a separate database.
    Column(
        'id',
        postgres.UUID(as_uuid=True),
        primary_key=True,
        comment='Dataset ID',
    ),
    Column(
        'dataset_type_ref',
        SmallInteger,
        comment='Cubedash product list '
                '(corresponding to datacube dataset_type)',
        nullable=False,
    ),
    Column('center_time', DateTime(timezone=True), nullable=False),

    # When was the dataset created? creation_time if it has one, otherwise datacube index time.
    Column('creation_time', DateTime(timezone=True), nullable=False),

    # Nullable: Some products have no region.
    Column('region_code', String, comment=''),

    # Size of this dataset in bytes, if the product includes it.
    Column('size_bytes', BigInteger),

    Column('footprint', Geometry(spatial_index=False)),

    # Default postgres naming conventions.
    Index(
        "dataset_spatial_dataset_type_ref_center_time_idx", 'dataset_type_ref', 'center_time'
    ),
    # Faster region pages. Could be removed if faster summary generation is desired...
    Index(
        "dataset_spatial_dataset_type_ref_region_code_idx",
        'dataset_type_ref', 'region_code',
        postgresql_ops={
            'region_code': 'text_pattern_ops',
        }
    ),
)

# Note that we deliberately don't foreign-key to datacube tables:
# - We don't want to add an external dependency on datacube core (breaking, eg, product deletion scripts)
# - they may be in a separate database.
PRODUCT = Table(
    'product', METADATA,
    Column('id', SmallInteger, primary_key=True),
    Column('name', String, unique=True, nullable=False),

    Column('dataset_count', Integer, nullable=False),

    Column(
        'last_refresh',
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Last refresh of this product in the dataset_spatial table",
    ),

    Column('source_product_refs', postgres.ARRAY(SmallInteger)),
    Column('derived_product_refs', postgres.ARRAY(SmallInteger)),

    Column('time_earliest', DateTime(timezone=True)),
    Column('time_latest', DateTime(timezone=True)),
)
TIME_OVERVIEW = Table(
    'time_overview', METADATA,
    # Uniquely identified by three values:
    Column('product_ref', None, ForeignKey(PRODUCT.c.id)),
    Column('period_type', Enum('all', 'year', 'month', 'day', name='overviewperiod')),
    Column('start_day', Date),

    Column('dataset_count', Integer, nullable=False),

    # Time range (if there's at least one dataset)
    Column('time_earliest', DateTime(timezone=True)),
    Column('time_latest', DateTime(timezone=True)),

    Column('timeline_period',
           Enum('year', 'month', 'week', 'day', name='timelineperiod'),
           nullable=False),

    Column(
        'timeline_dataset_start_days',
        postgres.ARRAY(DateTime(timezone=True)),
        nullable=False
    ),
    Column(
        'timeline_dataset_counts',
        postgres.ARRAY(Integer),
        nullable=False
    ),

    Column('regions', postgres.ARRAY(String), nullable=False),
    Column('region_dataset_counts', postgres.ARRAY(Integer), nullable=False),

    # The most newly created dataset
    Column(
        'newest_dataset_creation_time',
        DateTime(timezone=True)
    ),

    # When this summary was generated
    Column(
        'generation_time',
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    ),

    Column('footprint_count', Integer, nullable=False),
    Column('footprint_geometry', Geometry(srid=FOOTPRINT_SRID, spatial_index=False)),
    Column('crses', postgres.ARRAY(String)),

    # Size of this dataset in bytes, if the product includes it.
    Column('size_bytes', BigInteger),

    PrimaryKeyConstraint('product_ref', 'start_day', 'period_type'),
    CheckConstraint(
        r"array_length(timeline_dataset_start_days, 1) = "
        r"array_length(timeline_dataset_counts, 1)",
        name='timeline_lengths_equal'
    ),
)

_PG_GRIDCELL_STRING = re.compile(r"\(([^)]+),([^)]+)\)")


def create_schema(engine: Engine):
    engine.execute(DDL(f"create schema if not exists {CUBEDASH_SCHEMA}"))
    engine.execute(DDL(f"create extension if not exists postgis"))

    # We want an index on the spatial_ref_sys table to do authority name/code lookups.
    # But in RDS environments we cannot add indexes to it.
    # So we create our own copy as a materialised view (it's a very small table).
    engine.execute(f"""
    create materialized view if not exists {CUBEDASH_SCHEMA}.mv_spatial_ref_sys
        as select * from spatial_ref_sys;
    """)
    # The normal primary key.
    engine.execute(f"""
        create unique index if not exists mv_spatial_ref_sys_srid_idx on 
            {CUBEDASH_SCHEMA}.mv_spatial_ref_sys(srid);
        """)
    # For case insensitive auth name/code lookups.
    # (Postgis doesn't add one by default, but we're going to do a lot of lookups)
    engine.execute(f"""
        create unique index if not exists mv_spatial_ref_sys_lower_auth_srid_idx on 
            {CUBEDASH_SCHEMA}.mv_spatial_ref_sys(lower(auth_name::text), auth_srid);
    """)
    # Fallback to match the whole WKT text.
    engine.execute(f"""
            create unique index if not exists mv_spatial_ref_sys_srtext on 
                {CUBEDASH_SCHEMA}.mv_spatial_ref_sys(srtext);
        """)

    METADATA.create_all(engine, checkfirst=True)


def pg_exists(conn, name):
    """
    Does a postgres object exist?
    :rtype bool
    """
    return conn.execute("SELECT to_regclass(%s)", name).scalar() is not None
