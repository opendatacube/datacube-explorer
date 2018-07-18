import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime

import structlog
from click import echo, secho, style
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from psycopg2._range import Range as PgRange
from psycopg2.extensions import AsIs, adapt, register_adapter
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    bindparam,
    case,
    func,
    literal,
    null,
    select,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine
from sqlalchemy.types import UserDefinedType

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
    doc = _jsonb_doc_expression(md)

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    projection_offset = _projection_doc_offset(md)
    valid_data_offset = projection_offset + ["valid_data"]

    return func.ST_SetSRID(
        case(
            [
                # If we have valid_data offset, use it as the polygon.
                (
                    doc[valid_data_offset] != None,
                    func.ST_GeomFromGeoJSON(
                        doc[valid_data_offset].astext, type_=Geometry
                    ),
                )
            ],
            # Otherwise construct a polygon from the four corner points.
            else_=_bounds_polygon(doc, projection_offset),
        ),
        get_dataset_srid_alchemy_expression(md),
        type_=Geometry,
    )


def _projection_doc_offset(md):
    projection_offset = md.definition["dataset"]["grid_spatial"]
    return projection_offset


def _jsonb_doc_expression(md):
    doc = md.dataset_fields["metadata_doc"].alchemy_expression
    return doc


def _bounds_polygon(doc, projection_offset):
    geo_ref_points_offset = projection_offset + ["geo_ref_points"]
    return func.ST_MakePolygon(
        func.ST_MakeLine(
            postgres.array(
                tuple(
                    _gis_point(doc, geo_ref_points_offset + [key])
                    for key in ("ll", "ul", "ur", "lr", "ll")
                )
            )
        ),
        type_=Geometry,
    )


def _grid_point(dt: DatasetType):
    """
    Get an sqlalchemy expression to calculte the grid number of a dataset.

    Eg.
        On scenes this is the path/row
        On tiles this is the tile numbers

    Returns as a postgres array of small int.
    """
    grid_spec = dt.grid_spec

    md_fields = dt.metadata_type.dataset_fields

    # If the product has a grid spec, we can calculate the grid number
    if grid_spec is not None:
        doc = _jsonb_doc_expression(dt.metadata_type)
        projection_offset = _projection_doc_offset(dt.metadata_type)

        # Calculate tile refs
        center_point = func.ST_Centroid(_bounds_polygon(doc, projection_offset))

        # todo: look at grid_spec crs. Use it for defaults, conversion.
        size_x, size_y = grid_spec.tile_size or (1000.0, 1000.0)
        origin_x, origin_y = grid_spec.origin
        return func.point(
            func.floor((func.ST_X(center_point) - origin_x) / size_x),
            func.floor((func.ST_Y(center_point) - origin_y) / size_y),
            type_=PgPoint,
        )
    # Otherwise does the product have a "sat_path/sat_row" fields? Use their values directly.
    elif "sat_path" in md_fields:
        # Use sat_path/sat_row as grid items
        path_field: RangeDocField = md_fields["sat_path"]
        row_field: RangeDocField = md_fields["sat_row"]

        return func.point(
            path_field.lower.alchemy_expression,
            row_field.greater.alchemy_expression,
            type_=PgPoint,
        )
    else:
        _LOG.warn(
            "no_grid_spec",
            product_name=dt.name,
            metadata_type_name=dt.metadata_type.name,
        )
        return null()


def get_dataset_srid_alchemy_expression(md: MetadataType):
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    projection_offset = md.definition["dataset"]["grid_spatial"]

    # Most have a spatial_reference field we can use directly.
    spatial_reference_offset = projection_offset + ["spatial_reference"]
    spatial_ref = doc[spatial_reference_offset].astext
    return func.coalesce(
        case(
            [
                (
                    # If matches shorthand code: eg. "epsg:1234"
                    spatial_ref.op("~")(r"^[A-Za-z0-9]+:[0-9]+$"),
                    select([SPATIAL_REF_SYS.c.srid])
                    .where(
                        func.lower(SPATIAL_REF_SYS.c.auth_name)
                        == func.lower(func.split_part(spatial_ref, ":", 1))
                    )
                    .where(
                        SPATIAL_REF_SYS.c.auth_srid
                        == func.split_part(spatial_ref, ":", 2).cast(Integer)
                    )
                    .as_scalar(),
                )
            ],
            else_=None,
        ),
        # Some older datasets have datum/zone fields instead.
        # The only remaining ones in DEA are 'GDA94'.
        case(
            [
                (
                    doc[(projection_offset + ["datum"])].astext == "GDA94",
                    select([SPATIAL_REF_SYS.c.srid])
                    .where(SPATIAL_REF_SYS.c.auth_name == "EPSG")
                    .where(
                        SPATIAL_REF_SYS.c.auth_srid
                        == (
                            "283"
                            + func.abs(
                                doc[(projection_offset + ["zone"])].astext.cast(Integer)
                            )
                        ).cast(Integer)
                    )
                    .as_scalar(),
                )
            ],
            else_=None,
        )
        # TODO: third option: CRS as text/WKT
    )


def _gis_point(doc, doc_offset):
    return func.ST_MakePoint(
        doc[doc_offset + ["x"]].astext.cast(postgres.DOUBLE_PRECISION),
        doc[doc_offset + ["y"]].astext.cast(postgres.DOUBLE_PRECISION),
    )


@dataclass(frozen=True)
class Point(object):
    x: float
    y: float


def adapt_point(point):
    x = adapt(point.x).getquoted()
    y = adapt(point.y).getquoted()
    return AsIs("'(%s, %s)'" % (x, y))


register_adapter(Point, adapt_point)


class PgPoint(UserDefinedType):
    def get_col_spec(self):
        return "point"

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process


POSTGIS_METADATA = MetaData(schema="public")
SPATIAL_REF_SYS = Table(
    "spatial_ref_sys",
    POSTGIS_METADATA,
    Column("srid", Integer, primary_key=True),
    Column("auth_name", String(255)),
    Column("auth_srid", Integer),
    Column("srtext", String(2048)),
    Column("proj4text", String(2048)),
)

DATASET_SPATIAL = Table(
    "dataset_spatial",
    CUBEDASH_DB_METADATA,
    # Note that we deliberately don't foreign-key to datacube tables: they may
    # be in a separate database.
    Column("id", postgres.UUID(as_uuid=True), primary_key=True, comment="Dataset ID"),
    Column(
        "dataset_type_ref",
        None,
        ForeignKey(DATASET_TYPE.c.id),
        comment="Cubedash product list " "(corresponding to datacube dataset_type)",
        nullable=False,
    ),
    Column("time", TSTZRANGE),
    Column("footprint", Geometry(spatial_index=False)),
    Column("grid_point", PgPoint),
    # Column('native_srid', None, ForeignKey(SPATIAL_REF_SYS.c.srid)),
    # Column('bounds', Geometry()),
)


def add_spatial_table(dc: Datacube, *products: DatasetType):
    engine: Engine = dc.index.datasets._db._engine
    DATASET_SPATIAL.create(engine, checkfirst=True)

    _add_convenience_views(engine)

    for product in products:
        echo(
            f"{datetime.now()} "
            f"Starting {style(product.name, bold=True)} extent update"
        )
        insert_count = _insert_spatial_records(engine, product)
        echo(
            f"{datetime.now()} "
            f"Added {style(str(insert_count), bold=True)} new extents "
            f"for {style(product.name, bold=True)}. "
        )


def _add_convenience_views(engine):
    """
    Convenience view for seeing product names, sizes and readable geometry
    """
    engine.execute(
        """
    create or replace view cubedash.view_extents as 
    select 
      dt.name as product, 
      ST_AsEWKT(sizes.footprint) as footprint_ewkt, 
      sizes.* 
    from cubedash.dataset_spatial sizes 
    inner join agdc.dataset_type dt on sizes.dataset_type_ref = dt.id;
    """
    )

    engine.execute(
        """
    create or replace view cubedash.view_space_usage as (
        select 
            dt.name as product, 
            sizes.* 
        from (
            select
            dataset_type_ref,
            count(*),
            pg_size_pretty(sum(pg_column_size(id               ))) as id_col_size,
            pg_size_pretty(sum(pg_column_size(time             ))) as time_col_size,
            pg_size_pretty(sum(pg_column_size(footprint))) as footprint_col_size,
            pg_size_pretty(round(avg(pg_column_size(footprint)), 0)) as avg_footprint_col_size,
            count(*) filter (where (not ST_IsValid(footprint))) as invalid_footprints,
            count(*) filter (where (ST_SRID(footprint) is null)) as missing_srid
            from cubedash.dataset_spatial
            group by 1
        ) sizes
        inner join agdc.dataset_type dt on sizes.dataset_type_ref = dt.id
        order by sizes.count desc
    );
    """
    )

    engine.execute(
        """
    create materialized view if not exists cubedash.product_extent as (
        select dataset_type_ref, 
               tstzrange(min(lower(time)), max(upper(time))), 
               ST_Extent(footprint)
        from cubedash.dataset_spatial 
        group by 1
    );
    """
    )


def _insert_spatial_records(engine: Engine, product: DatasetType):
    product_ref = bindparam("product_ref", product.id, type_=SmallInteger)
    query = (
        postgres.insert(DATASET_SPATIAL)
        .from_select(
            ["id", "dataset_type_ref", "time", "footprint", "grid_point"],
            _select_dataset_extent_query(product)
            .where(DATASET.c.dataset_type_ref == product_ref)
            .where(DATASET.c.archived == None),
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )

    _LOG.debug(
        "spatial_insert_query", product_name=product.name, query_sql=as_sql(query)
    )

    return engine.execute(query).rowcount


def _select_dataset_extent_query(dt: DatasetType):
    md_type = dt.metadata_type
    # If this product has lat/lon fields, we can take spatial bounds.

    footrprint_expression = get_dataset_extent_alchemy_expression(md_type)
    return select(
        [
            DATASET.c.id,
            DATASET.c.dataset_type_ref,
            (md_type.dataset_fields["time"].alchemy_expression).label("time"),
            (null() if footrprint_expression is None else footrprint_expression).label(
                "footprint"
            ),
            _grid_point(dt).label("grid_point"),
        ]
    ).select_from(DATASET)


def get_dataset_bounds_query(md_type):
    if "lat" not in md_type.dataset_fields:
        # Not a spatial product
        return None

    lat, lon = md_type.dataset_fields["lat"], md_type.dataset_fields["lon"]
    assert isinstance(lat, RangeDocField)
    assert isinstance(lon, RangeDocField)
    return func.ST_MakeBox2D(
        func.ST_MakePoint(lat.lower.alchemy_expression, lon.lower.alchemy_expression),
        func.ST_MakePoint(
            lat.greater.alchemy_expression, lon.greater.alchemy_expression
        ),
        type_=Geometry,
    )


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


def print_query_tests(dc: Datacube, *products: DatasetType):
    engine: Engine = dc.index.datasets._db._engine
    DATASET_SPATIAL.create(engine, checkfirst=True)

    def show(title, output):
        secho(f"=== {title} ({product.name}) ===", bold=True, err=True)
        echo(output, err=True)
        secho(f"=== End {title} ===", bold=True, err=True)

    for product in products:
        product_ref = bindparam("product_ref", product.id, type_=SmallInteger)
        one_dataset_query = (
            _select_dataset_extent_query(product)
            .where(DATASET.c.dataset_type_ref == product_ref)
            .where(DATASET.c.archived == None)
            .limit(1)
        )

        # Look at the raw query being generated.
        # This is not very readable, but can be copied into PyCharm or
        # equivalent for formatting.
        if DEBUG:
            show("Raw Query", as_sql(one_dataset_query, product_ref=product.id))

        # Print an example extent row
        ret = engine.execute(one_dataset_query).fetchall()
        if len(ret) == 1:
            dataset_row = ret[0]
            show("Example dataset", _as_json(dict(dataset_row)))
        else:
            show("No datasets", "<empty>")


def _as_json(obj):
    def fallback(o, *args, **kwargs):
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, WKBElement):
            # Following the EWKT format: include srid
            prefix = f"SRID={o.srid};" if o.srid else ""
            return prefix + to_shape(o).wkt
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, PgRange):
            return ["∞" if o.lower_inf else o.lower, "∞" if o.upper_inf else o.upper]
        return repr(o)

    return json.dumps(obj, indent=4, default=fallback)


DEBUG = False
if __name__ == "__main__":
    with Datacube(env="clone") as dc:
        products = [
            p for p in dc.index.products.get_all() if p.name.startswith(sys.argv[1])
        ]

        # Sample one of each product. Useful to find errors immediately.
        print_query_tests(dc, *products)
        # Populate whole table
        if not DEBUG:
            add_spatial_table(dc, *products)
