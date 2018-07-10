from datetime import datetime
from geoalchemy2 import Geometry
from sqlalchemy import func, case, cast, select, Table, Column, ForeignKey, String, \
    bindparam, Integer
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine

from cubedash.summary._stores import METADATA as CUBEDASH_DB_METADATA
from datacube import Datacube
from datacube.drivers.postgres._schema import DATASET, DATASET_TYPE
from datacube.model import MetadataType


def get_dataset_extent_alchemy_expression(md: MetadataType):
    """
    Build an SQLaLchemy expression to get the extent for a dataset.

    It's returned as a postgis geometry.

    The logic here mirrors the extent() function of datacube.model.Dataset.
    """
    doc = md.dataset_fields['metadata_doc'].alchemy_expression

    projection_offset = md.definition['dataset']['grid_spatial']

    projection = doc[projection_offset]

    valid_data_offset = projection_offset + ['valid_data']
    geo_ref_points_offset = projection_offset + ['geo_ref_points']

    # If we have valid_data offset, return it as a polygon.
    return case(
        [
            (
                projection[valid_data_offset] != None,
                func.ST_GeomFromGeoJSON(
                    doc[valid_data_offset].astext,
                    type_=Geometry
                )
            ),
        ],
        # Otherwise construct a polygon from the four corner points.
        else_=func.ST_MakePolygon(
            func.ST_MakeLine(
                postgres.array(tuple(
                    _gis_point(doc[geo_ref_points_offset + [key]])
                    for key in ('ll', 'ul', 'ur', 'lr', 'll')
                ))
            ), type_=Geometry
        ),

    )


def get_dataset_crs_alchemy_expression(md: MetadataType):
    doc = md.dataset_fields['metadata_doc'].alchemy_expression

    projection_offset = md.definition['dataset']['grid_spatial']

    # Most have a spatial_reference field we can use directly.
    spatial_reference_offset = projection_offset + ['spatial_reference']
    return func.coalesce(
        doc[spatial_reference_offset].astext,
        # Some older datasets have datum/zone fields instead.
        # The only remaining ones in DEA are 'GDA94'.
        case(
            [
                (
                    doc[(projection_offset + ['datum'])].astext == 'GDA94',
                    'EPSG:283' + func.abs(
                        cast(doc[(projection_offset + ['zone'])].astext, Integer))
                )
            ],
            else_=None
        )
    )


def _gis_point(obj):
    return func.ST_MakePoint(
        cast(obj['x'].astext, postgres.DOUBLE_PRECISION),
        cast(obj['y'].astext, postgres.DOUBLE_PRECISION)
    )


DATASET_SPATIAL = Table(
    'dataset_spatial',
    CUBEDASH_DB_METADATA,
    # Note that we deliberately don't foreign-key to datacube tables: they may
    # be in a separate database.
    Column(
        'id',
        postgres.UUID(as_uuid=True),
        primary_key=True,
        comment='Dataset ID',
    ),
    Column(
        'product_ref',
        None,
        ForeignKey(DATASET_TYPE.c.id),
        comment='Cubedash product list '
                '(corresponding to datacube dataset_type)',
        nullable=False,
    ),
    Column('time', TSTZRANGE),
    Column('extent', Geometry()),
    Column('crs', String),
)


def add_spatial_table():

    with Datacube(env='clone') as dc:
        engine: Engine = dc.index.datasets._db._engine
        DATASET_SPATIAL.create(engine, checkfirst=True)

        eo_type = dc.index.metadata_types.get_by_name('eo')
        print(f"Starting spatial insertion for {eo_type.name}. {datetime.now()}")
        insert_count = _insert_spatial_records(engine, eo_type)
        print(
            f"Added {insert_count} dataset records for {eo_type.name}."
            f"{datetime.now()}"
        )


def _insert_spatial_records(engine: Engine, md_type: MetadataType):
    ret = engine.execute(
        DATASET_SPATIAL.insert().from_select(
            ['id', 'product_ref', 'time', 'extent', 'crs'],
            select([
                DATASET.c.id,
                DATASET.c.dataset_type_ref,
                md_type.dataset_fields['time'].alchemy_expression.label('time'),
                get_dataset_extent_alchemy_expression(md_type).label('extent'),
                get_dataset_crs_alchemy_expression(md_type).label('crs'),
            ]).select_from(DATASET).where(
                DATASET.c.metadata_type_ref == bindparam('metadata_type_ref')
            )
        ),
        metadata_type_ref=md_type.id
    )
    return ret.rowcount


if __name__ == '__main__':
    add_spatial_table()
