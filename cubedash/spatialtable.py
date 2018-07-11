import json
import uuid
from datetime import datetime

import structlog
from click import echo, secho, style
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from psycopg2._range import Range as PgRange
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Table,
    bindparam,
    case,
    func,
    select,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine

from cubedash.summary._stores import METADATA as CUBEDASH_DB_METADATA
from datacube import Datacube
from datacube.drivers.postgres._fields import RangeDocField
from datacube.drivers.postgres._schema import DATASET, DATASET_TYPE
from datacube.model import DatasetType, MetadataType

_LOG = structlog.get_logger()


def get_dataset_extent_alchemy_expression(md: MetadataType):
    """
    Build an SQLaLchemy expression to get the extent for a dataset.

    It's returned as a postgis geometry.

    The logic here mirrors the extent() function of datacube.model.Dataset.
    """
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    projection_offset = md.definition["dataset"]["grid_spatial"]
    valid_data_offset = projection_offset + ["valid_data"]
    geo_ref_points_offset = projection_offset + ["geo_ref_points"]

    # If we have valid_data offset, return it as a polygon.
    return case(
        [
            (
                doc[valid_data_offset] != None,
                func.ST_GeomFromGeoJSON(doc[valid_data_offset].astext, type_=Geometry),
            )
        ],
        # Otherwise construct a polygon from the four corner points.
        else_=func.ST_MakePolygon(
            func.ST_MakeLine(
                postgres.array(
                    tuple(
                        _gis_point(doc, geo_ref_points_offset + [key])
                        for key in ("ll", "ul", "ur", "lr", "ll")
                    )
                )
            ),
            type_=Geometry,
        ),
    )


def get_dataset_crs_alchemy_expression(md: MetadataType):
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    projection_offset = md.definition["dataset"]["grid_spatial"]

    # Most have a spatial_reference field we can use directly.
    spatial_reference_offset = projection_offset + ["spatial_reference"]
    return func.coalesce(
        doc[spatial_reference_offset].astext,
        # Some older datasets have datum/zone fields instead.
        # The only remaining ones in DEA are 'GDA94'.
        case(
            [
                (
                    doc[(projection_offset + ["datum"])].astext == "GDA94",
                    "EPSG:283"
                    + func.abs(
                        doc[(projection_offset + ["zone"])].astext.cast(Integer)
                    ),
                )
            ],
            else_=None,
        ),
    )


def _gis_point(doc, doc_offset):
    return func.ST_MakePoint(
        doc[doc_offset + ["x"]].astext.cast(postgres.DOUBLE_PRECISION),
        doc[doc_offset + ["y"]].astext.cast(postgres.DOUBLE_PRECISION),
    )


DATASET_SPATIAL = Table(
    "dataset_spatial",
    CUBEDASH_DB_METADATA,
    # Note that we deliberately don't foreign-key to datacube tables: they may
    # be in a separate database.
    Column("id", postgres.UUID(as_uuid=True), primary_key=True, comment="Dataset ID"),
    Column(
        "product_ref",
        None,
        ForeignKey(DATASET_TYPE.c.id),
        comment="Cubedash product list " "(corresponding to datacube dataset_type)",
        nullable=False,
    ),
    Column("time", TSTZRANGE),
    Column("extent", Geometry()),
    Column("crs", String),
)


def add_spatial_table(*product_names):

    with Datacube(env="clone") as dc:
        engine: Engine = dc.index.datasets._db._engine
        DATASET_SPATIAL.create(engine, checkfirst=True)

        for product_name in product_names:
            product = dc.index.products.get_by_name(product_name)

            echo(
                f"{datetime.now()}"
                f"Starting {style(product.name, bold=True)} extent update"
            )
            insert_count = _insert_spatial_records(engine, product)
            echo(
                f"{datetime.now()} "
                f"Added {style(str(insert_count), bold=True)} new extents "
                f"for {style(product.name, bold=True)}. "
            )


def _insert_spatial_records(engine: Engine, product: DatasetType):
    product_ref = bindparam("product_ref", product.id, type_=SmallInteger)
    query = (
        postgres.insert(DATASET_SPATIAL)
        .from_select(
            ["id", "product_ref", "time", "extent", "crs"],
            _select_dataset_extent_query(product.metadata_type)
            .where(DATASET.c.dataset_type_ref == product_ref)
            .where(DATASET.c.archived == None),
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )

    _LOG.debug(
        "spatial_insert_query", product_name=product.name, query_sql=as_sql(query)
    )

    return engine.execute(query).rowcount


def _select_dataset_extent_query(md_type):
    return select(
        [
            DATASET.c.id,
            DATASET.c.dataset_type_ref,
            md_type.dataset_fields["time"].alchemy_expression.label("time"),
            get_dataset_extent_alchemy_expression(md_type).label("extent"),
            get_dataset_crs_alchemy_expression(md_type).label("crs"),
        ]
    ).select_from(DATASET)


def as_sql(expression, **params):
    """Convert sqlalchemy expression to SQL string.

    (primarily for debugging: to see what sqlalchemy is doing)

    This has its literal values bound, so it's more readable than the engine's
    query logging.
    """
    if params:
        expression = expression.params(**params)
    return str(
        expression.compile(
            dialect=postgres.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def print_query_tests(*product_names):
    with Datacube(env="clone") as dc:
        engine: Engine = dc.index.datasets._db._engine
        DATASET_SPATIAL.create(engine, checkfirst=True)

        def show(title, output):
            secho(f"=== {title} ({product_name}) ===", bold=True)
            echo(output)
            secho(f"=== End {title} ===", bold=True)

        for product_name in product_names:
            product = dc.index.products.get_by_name(product_name)

            product_ref = bindparam("product_ref", product.id, type_=SmallInteger)
            one_dataset_query = (
                _select_dataset_extent_query(product.metadata_type)
                .where(DATASET.c.dataset_type_ref == product_ref)
                .where(DATASET.c.archived == None)
                .limit(1)
            )

            # Look at the raw query being generated.
            # This is not very readable, but can be copied into PyCharm or
            # equivalent for formatting.
            show("Raw Query", as_sql(one_dataset_query, product_ref=product.id))

            # Print an example extent row
            ret = engine.execute(one_dataset_query).fetchall()
            assert len(ret) == 1
            dataset_row = ret[0]
            show("Example dataset", _as_json(dict(dataset_row)))


def _as_json(obj):
    def fallback(o, *args, **kwargs):
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, WKBElement):
            return to_shape(o).wkt
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, PgRange):
            return ["∞" if o.lower_inf else o.lower, "∞" if o.upper_inf else o.upper]
        return repr(o)

    return json.dumps(obj, indent=4, default=fallback)


if __name__ == "__main__":
    print_query_tests("ls8_nbar_albers", "ls8_level1_scene")
    add_spatial_table("ls8_nbar_albers", "ls8_level1_scene")
