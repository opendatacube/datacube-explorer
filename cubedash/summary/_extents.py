import functools
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import fiona
import shapely.wkb
import structlog
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from psycopg2._range import Range as PgRange
from shapely.geometry import GeometryCollection, shape
from sqlalchemy import (
    BigInteger,
    Integer,
    SmallInteger,
    String,
    bindparam,
    case,
    func,
    null,
    select,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ColumnElement

import datacube.drivers.postgres._api as postgres_api
from cubedash._utils import alchemy_engine
from cubedash.summary._schema import DATASET_SPATIAL, SPATIAL_REF_SYS
from datacube import Datacube
from datacube.drivers.postgres._fields import PgDocField, RangeDocField
from datacube.drivers.postgres._schema import DATASET
from datacube.index import Index
from datacube.model import DatasetType, MetadataType

_LOG = structlog.get_logger()

_WRS_PATH_ROW = [
    Path(__file__).parent.parent / "data" / "WRS2_descending" / "WRS2_descending.shp",
    Path(__file__).parent.parent / "data" / "WRS2_ascending" / "WRS2_acsending.shp",
]


def get_dataset_extent_alchemy_expression(md: MetadataType, default_crs: str = None):
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
        get_dataset_srid_alchemy_expression(md, default_crs),
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


def _size_bytes_field(dt: DatasetType):
    md_fields = dt.metadata_type.dataset_fields
    if "size_bytes" in md_fields:
        return md_fields["size_bytes"].alchemy_expression

    return _jsonb_doc_expression(dt.metadata_type)["size_bytes"].astext.cast(BigInteger)


def get_dataset_srid_alchemy_expression(md: MetadataType, default_crs: str = None):
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    projection_offset = md.definition["dataset"]["grid_spatial"]

    # Most have a spatial_reference field we can use directly.
    spatial_reference_offset = projection_offset + ["spatial_reference"]
    spatial_ref = doc[spatial_reference_offset].astext

    # When datasets have no CRS, optionally use this as default.
    default_crs_expression = None
    if default_crs:
        if not default_crs.lower().startswith("epsg:"):
            raise NotImplementedError(
                "CRS expected in form of 'EPSG:1234'. Got: %r" % default_crs
            )

        auth_name, auth_srid = default_crs.split(":")
        default_crs_expression = (
            select([SPATIAL_REF_SYS.c.srid])
            .where(func.lower(SPATIAL_REF_SYS.c.auth_name) == auth_name.lower())
            .where(SPATIAL_REF_SYS.c.auth_srid == int(auth_srid))
            .as_scalar()
        )

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
            else_=default_crs_expression,
        ),
        # Some older datasets have datum/zone fields instead.
        # The only remaining ones in DEA are 'GDA94'.
        case(
            [
                (
                    doc[(projection_offset + ["datum"])].astext == "GDA94",
                    select([SPATIAL_REF_SYS.c.srid])
                    .where(func.lower(SPATIAL_REF_SYS.c.auth_name) == "epsg")
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
        ),
        # TODO: third option: CRS as text/WKT
    )


def _gis_point(doc, doc_offset):
    return func.ST_MakePoint(
        doc[doc_offset + ["x"]].astext.cast(postgres.DOUBLE_PRECISION),
        doc[doc_offset + ["y"]].astext.cast(postgres.DOUBLE_PRECISION),
    )


def refresh_product(index: Index, product: DatasetType):
    engine: Engine = alchemy_engine(index)
    insert_count = _populate_missing_dataset_extents(engine, product)
    return insert_count


def _populate_missing_dataset_extents(engine: Engine, product: DatasetType):
    query = (
        postgres.insert(DATASET_SPATIAL)
        .from_select(
            [
                "id",
                "dataset_type_ref",
                "center_time",
                "footprint",
                "region_code",
                "size_bytes",
                "creation_time",
            ],
            _select_dataset_extent_query(product),
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )

    _LOG.debug(
        "spatial_insert_query.start",
        product_name=product.name,
        # query_sql=as_sql(query),
    )
    inserted = engine.execute(query).rowcount
    _LOG.debug("spatial_insert_query.end", product_name=product.name, inserted=inserted)
    return inserted


def _select_dataset_extent_query(dt: DatasetType):
    md_type = dt.metadata_type
    # If this product has lat/lon fields, we can take spatial bounds.

    footrprint_expression = get_dataset_extent_alchemy_expression(
        md_type, default_crs=_default_crs(dt)
    )
    product_ref = bindparam("product_ref", dt.id, type_=SmallInteger)

    # "expr == None" is valid in sqlalchemy:
    # pylint: disable=singleton-comparison
    time = md_type.dataset_fields["time"].alchemy_expression
    return (
        select(
            [
                DATASET.c.id,
                DATASET.c.dataset_type_ref,
                (func.lower(time) + (func.upper(time) - func.lower(time)) / 2).label(
                    "center_time"
                ),
                (
                    null() if footrprint_expression is None else footrprint_expression
                ).label("footprint"),
                _region_code_field(dt).label("region_code"),
                _size_bytes_field(dt).label("size_bytes"),
                _dataset_creation_expression(md_type).label("creation_time"),
            ]
        )
        .where(DATASET.c.dataset_type_ref == product_ref)
        .where(DATASET.c.archived == None)
    )


def _default_crs(dt: DatasetType) -> Optional[str]:
    storage = dt.definition.get("storage")
    if not storage:
        return None

    return storage.get("crs")


def _dataset_creation_expression(md: MetadataType) -> Optional[datetime]:
    """SQLAlchemy expression for the creation (processing) time of a dataset"""

    # Either there's a field called "created", or we fallback to the default "creation_dt' in metadata type.
    created_field = md.dataset_fields.get("created")
    if created_field is not None:
        assert isinstance(created_field, PgDocField)
        return created_field.alchemy_expression

    doc = md.dataset_fields["metadata_doc"].alchemy_expression
    creation_dt = md.definition["dataset"].get("creation_dt") or ["creation_dt"]
    return func.agdc.common_timestamp(doc[creation_dt].astext)


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


def print_sample_dataset(*product_names: str):
    with Datacube(env="clone") as dc:
        index = dc.index
        for product_name in product_names:
            product = index.products.get_by_name(product_name)
            res = (
                alchemy_engine(index)
                .execute(_select_dataset_extent_query(product).limit(1))
                .fetchone()
            )
            print(_as_json(dict(res)))


# This is tied to ODC's internal Dataset search implementation as there's no higher-level api to allow this.
# When region_code is integrated into core (as is being discussed) this can be replaced.
# pylint: disable=protected-access
def datasets_by_region(engine, index, product_name, region_code, time_range, limit):
    product = index.products.get_by_name(product_name)
    query = (
        select(postgres_api._DATASET_SELECT_FIELDS)
        .select_from(
            DATASET_SPATIAL.join(DATASET, DATASET_SPATIAL.c.id == DATASET.c.id)
        )
        .where(DATASET_SPATIAL.c.region_code == bindparam("region_code", region_code))
        .where(
            DATASET_SPATIAL.c.dataset_type_ref
            == bindparam("dataset_type_ref", product.id)
        )
    )
    if time_range:
        query = query.where(
            DATASET_SPATIAL.c.center_time > bindparam("from_time", time_range.begin)
        ).where(DATASET_SPATIAL.c.center_time < bindparam("to_time", time_range.end))
    query = query.order_by(DATASET_SPATIAL.c.center_time).limit(
        bindparam("limit", limit)
    )

    return (
        index.datasets._make(res, full_info=True)
        for res in engine.execute(query).fetchall()
    )


class RegionInfo:
    def __init__(self, product: DatasetType) -> None:
        self.product = product

    # Treated as an "id" in view code. What kind of region?
    name: str = "region"
    # A human-readable description displayed on a UI.
    description: str = "Regions"
    # Used when printing counts "1 region", "5 regions".
    unit_label: str = "region"
    units_label: str = "regions"

    @classmethod
    def for_product(cls, dt: DatasetType):
        grid_spec = dt.grid_spec

        # hltc has a grid spec, but most attributes are missing, so grid_spec functions fail.
        # Therefore: only assume there's a grid if tile_size is specified.
        if grid_spec is not None and grid_spec.tile_size:
            return GridRegionInfo(dt)
        elif "sat_path" in dt.metadata_type.dataset_fields:
            return SceneRegionInfo(dt)
        # TODO: Geometry for other types of regions (eg. MGRS, or manual 'region_code' fields)
        return None

    def alchemy_expression(self) -> ColumnElement:
        raise NotImplementedError("alchemy expression", self.__class__.name)

    def geographic_extent(self, region_code: str) -> GeometryCollection:
        """
        Shape
        """
        raise NotImplementedError("alchemy expression", self.__class__.name)

    def region_label(self, region_code: str) -> str:
        """
        Convert the region_code into something human-readable.
        """
        return region_code


class GridRegionInfo(RegionInfo):
    name = "tiled"
    description = "Tiled product"
    unit_label = "tile"
    units_label = "tiles"

    def alchemy_expression(self):
        dt = self.product
        grid_spec = self.product.grid_spec

        doc = _jsonb_doc_expression(dt.metadata_type)
        projection_offset = _projection_doc_offset(dt.metadata_type)

        # Calculate tile refs
        geo_ref_points_offset = projection_offset + ["geo_ref_points"]
        center_point = func.ST_Centroid(
            func.ST_Collect(
                _gis_point(doc, geo_ref_points_offset + ["ll"]),
                _gis_point(doc, geo_ref_points_offset + ["ur"]),
            )
        )

        # todo: look at grid_spec crs. Use it for defaults, conversion.
        size_x, size_y = grid_spec.tile_size or (1000.0, 1000.0)
        origin_x, origin_y = grid_spec.origin
        return func.concat(
            func.floor((func.ST_X(center_point) - origin_x) / size_x).cast(String),
            "_",
            func.floor((func.ST_Y(center_point) - origin_y) / size_y).cast(String),
        )

    def geographic_extent(self, region_code: str) -> GeometryCollection:
        """
        Get a whole polygon for a gridcell
        """
        extent = self.product.grid_spec.tile_geobox(
            _from_xy_region_code(region_code)
        ).geographic_extent
        # TODO: The ODC Geometry __geo_interface__ breaks for some products
        # (eg, when the inner type is a GeometryCollection?)
        # So we're now converting to shapely to do it.
        # TODO: Is there a nicer way to do this?
        # pylint: disable=protected-access
        return shapely.wkb.loads(extent._geom.ExportToWkb())

    def region_label(self, region_code: str) -> str:
        return "Tile %+d, %+d" % _from_xy_region_code(region_code)


def _from_xy_region_code(region_code: str):
    """
    >>> _from_xy_region_code('95_3')
    (95, 3)
    >>> _from_xy_region_code('95_-3')
    (95, -3)
    """
    x, y = region_code.split("_")
    return int(x), int(y)


class SceneRegionInfo(RegionInfo):
    name = "scenes"
    description = "Landsat WRS scene-based product"
    unit_label = "scene"
    units_label = "scenes"

    def __init__(self, product: DatasetType) -> None:
        super().__init__(product)
        self.path_row_shapes = _get_path_row_shapes()

    def alchemy_expression(self):
        """
        Use sat_path/sat_row as grid items
        """
        md_fields = self.product.metadata_type.dataset_fields
        path_field: RangeDocField = md_fields["sat_path"]
        row_field: RangeDocField = md_fields["sat_row"]
        return func.concat(
            path_field.lower.alchemy_expression.cast(String),
            "_",
            row_field.greater.alchemy_expression.cast(String),
        )

    def geographic_extent(self, region_code: str) -> GeometryCollection:
        return self.path_row_shapes[_from_xy_region_code(region_code)]

    def region_label(self, region_code: str) -> str:
        x, y = _from_xy_region_code(region_code)
        return f"Path {x}, Row {y}"


def _region_code_field(dt: DatasetType):
    """
    Get an sqlalchemy expression to calculate the region code (a string)

    Eg.
        On Landsat scenes this is the path/row (separated by underscore)
        On tiles this is the tile numbers (separated by underscore: possibly with negative)
        On Sentinel this is MGRS number
    """
    region_info = RegionInfo.for_product(dt)
    if region_info:
        return region_info.alchemy_expression()
    else:
        _LOG.warn(
            "no_region_code",
            product_name=dt.name,
            metadata_type_name=dt.metadata_type.name,
        )
        return null()


@functools.lru_cache()
def _get_path_row_shapes():
    path_row_shapes = {}
    for shape_file in _WRS_PATH_ROW:
        with fiona.open(str(shape_file)) as f:
            for k, item in f.items():
                prop = item["properties"]
                key = prop["PATH"], prop["ROW"]
                assert key not in path_row_shapes
                path_row_shapes[key] = shape(item["geometry"])
    return path_row_shapes


if __name__ == "__main__":
    print_sample_dataset(*(sys.argv[1:] or ["ls8_nbar_scene", "ls8_nbar_albers"]))
