import functools
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, List

import fiona
import shapely.ops
import structlog
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape, from_shape
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
    literal,
    null,
    select,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import ClauseElement, Label

import datacube.drivers.postgres._api as postgres_api
from cubedash._utils import alchemy_engine, infer_crs, ODC_DATASET as DATASET
from cubedash.summary._schema import DATASET_SPATIAL, SPATIAL_REF_SYS
from datacube import Datacube
from datacube.drivers.postgres._fields import PgDocField, RangeDocField

from datacube.index import Index
from datacube.model import DatasetType, Field, MetadataType, Dataset

_LOG = structlog.get_logger()

_WRS_PATH_ROW = [
    Path(__file__).parent.parent / "data" / "WRS2_descending" / "WRS2_descending.shp",
    Path(__file__).parent.parent / "data" / "WRS2_ascending" / "WRS2_acsending.shp",
]


def get_dataset_extent_alchemy_expression(md: MetadataType, default_crs: str = None):
    """
    Build an SQLAlchemy expression to get the extent for a dataset.

    It's returned as a postgis geometry.

    The logic here mirrors the extent() function of datacube.model.Dataset.
    """
    doc = _jsonb_doc_expression(md)

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    if expects_eo3_metadata_type(md):
        return func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(doc[["geometry"]], type_=Geometry),
            get_dataset_srid_alchemy_expression(md, default_crs),
        )
    else:
        projection_offset = _projection_doc_offset(md)
        valid_data_offset = projection_offset + ["valid_data"]

        return func.ST_SetSRID(
            case(
                [
                    # If we have valid_data offset, use it as the polygon.
                    (
                        doc[valid_data_offset] != None,
                        func.ST_GeomFromGeoJSON(doc[valid_data_offset], type_=Geometry),
                    )
                ],
                # Otherwise construct a polygon from the four corner points.
                else_=_bounds_polygon(doc, projection_offset),
            ),
            get_dataset_srid_alchemy_expression(md, default_crs),
            type_=Geometry,
        )


def expects_eo3_metadata_type(md: MetadataType) -> bool:
    """
    Does the given metadata type expect EO3 datasets?
    """
    # We don't have a clean way to say that a product expects EO3

    measurements_offset = md.definition["dataset"]["measurements"]

    # In EO3, the measurements are in ['measurments'],
    # In EO1, they are in ['image', 'bands'].
    return measurements_offset == ["measurements"]


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

    if expects_eo3_metadata_type(md):
        spatial_ref = doc[["crs"]].astext
    else:
        # Most have a spatial_reference field we can use directly.
        spatial_ref = doc[projection_offset + ["spatial_reference"]].astext

    # When datasets have no CRS, optionally use this as default.
    default_crs_expression = None
    if default_crs:
        if not default_crs.lower().startswith(
            "epsg:"
        ) and not default_crs.lower().startswith("esri:"):
            # HACK: Change default CRS with inference
            inferred_crs = infer_crs(default_crs)
            if inferred_crs is None:
                raise NotImplementedError(
                    f"CRS expected in form of 'EPSG:1234'. Got: {default_crs!r}"
                )
            default_crs = inferred_crs

        auth_name, auth_srid = default_crs.split(":")
        default_crs_expression = (
            select([SPATIAL_REF_SYS.c.srid])
            .where(func.lower(SPATIAL_REF_SYS.c.auth_name) == auth_name.lower())
            .where(SPATIAL_REF_SYS.c.auth_srid == int(auth_srid))
            .as_scalar()
        )

    expression = func.coalesce(
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
        case(
            [
                (
                    # Plain WKT that ends in an authority code.
                    # Extract the authority name and code using regexp. Yuck!
                    # Eg: ".... AUTHORITY["EPSG","32756"]]"
                    spatial_ref.op("~")(r'AUTHORITY\["[a-zA-Z0-9]+", *"[0-9]+"\]\]$'),
                    select([SPATIAL_REF_SYS.c.srid])
                    .where(
                        func.lower(SPATIAL_REF_SYS.c.auth_name)
                        == func.lower(
                            func.substring(
                                spatial_ref,
                                r'AUTHORITY\["([a-zA-Z0-9]+)", *"[0-9]+"\]\]$',
                            )
                        )
                    )
                    .where(
                        SPATIAL_REF_SYS.c.auth_srid
                        == func.substring(
                            spatial_ref, r'AUTHORITY\["[a-zA-Z0-9]+", *"([0-9]+)"\]\]$'
                        ).cast(Integer)
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
        default_crs_expression,
        # TODO: Handle arbitrary WKT strings (?)
        # 'GEOGCS[\\"GEOCENTRIC DATUM of AUSTRALIA\\",DATUM[\\"GDA94\\",SPHEROID[
        #    \\"GRS80\\",6378137,298.257222101]],PRIMEM[\\"Greenwich\\",0],UNIT[\\
        # "degree\\",0.0174532925199433]]'
    )
    # print(as_sql(expression))
    return expression


def _gis_point(doc, doc_offset):
    return func.ST_MakePoint(
        doc[doc_offset + ["x"]].astext.cast(postgres.DOUBLE_PRECISION),
        doc[doc_offset + ["y"]].astext.cast(postgres.DOUBLE_PRECISION),
    )


def refresh_product(index: Index, product: DatasetType, recompute_all_extents=False):
    """
    Record the spatial extents for each dataset in a product.

    By default, it will only add datasets that are currently missing.

    :param recompute_all_extents: replace all extents, even if already recorded
    """
    engine: Engine = alchemy_engine(index)
    insert_count = _populate_missing_dataset_extents(
        engine, product, force_update_all=recompute_all_extents
    )

    # If we inserted data...
    if insert_count:
        # And it's a non-spatial product...
        if get_dataset_extent_alchemy_expression(product.metadata_type) is None:
            # And it has WRS path/rows...
            if "sat_path" in product.metadata_type.dataset_fields:

                # We can synthesize the polygons!
                _LOG.debug(
                    "spatial_synthesizing.start", product_name=product.name,
                )
                shapes = _get_path_row_shapes()
                rows = [
                    row
                    for row in index.datasets.search_returning(
                        ("id", "sat_path", "sat_row"), product=product.name
                    )
                    if row.sat_path.lower is not None
                ]
                if rows:
                    engine.execute(
                        DATASET_SPATIAL.update()
                        .where(DATASET_SPATIAL.c.id == bindparam("dataset_id"))
                        .values(footprint=bindparam("footprint")),
                        [
                            dict(
                                dataset_id=id_,
                                footprint=from_shape(
                                    shapely.ops.unary_union(
                                        [
                                            shapes[(int(sat_path.lower), row)]
                                            for row in range(
                                                int(sat_row.lower),
                                                int(sat_row.upper) + 1,
                                            )
                                        ]
                                    ),
                                    srid=4326,
                                    extended=True,
                                ),
                            )
                            for id_, sat_path, sat_row in rows
                        ],
                    )
            _LOG.debug(
                "spatial_synthesizing.done", product_name=product.name,
            )

    return insert_count


def _populate_missing_dataset_extents(
    engine: Engine, product: DatasetType, force_update_all=False
):
    columns = {c.name: c for c in _select_dataset_extent_columns(product)}

    if force_update_all:
        query = (
            DATASET_SPATIAL.update()
            .values(**columns)
            .where(DATASET_SPATIAL.c.id == columns["id"])
            .where(
                DATASET.c.dataset_type_ref
                == bindparam("product_ref", product.id, type_=SmallInteger)
            )
            .where(DATASET.c.archived == None)
        )
    else:
        query = (
            postgres.insert(DATASET_SPATIAL)
            .from_select(
                columns.keys(),
                select(columns.values())
                .where(
                    DATASET.c.dataset_type_ref
                    == bindparam("product_ref", product.id, type_=SmallInteger)
                )
                .where(DATASET.c.archived == None)
                .order_by(columns["center_time"]),
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
    # print(as_sql(query))

    _LOG.debug(
        "spatial_insert_query.start",
        product_name=product.name,
        force_update_all=force_update_all,
    )
    changed = engine.execute(query).rowcount
    _LOG.debug(
        "spatial_insert_query.end", product_name=product.name, change_count=changed
    )
    return changed


def _select_dataset_extent_columns(dt: DatasetType) -> List[Label]:
    """
    Get columns for all fields which go into the spatial table
    for this DatasetType.
    """
    md_type = dt.metadata_type
    # If this product has lat/lon fields, we can take spatial bounds.

    footprint_expression = get_dataset_extent_alchemy_expression(
        md_type, default_crs=_default_crs(dt)
    )

    # Some time-series-derived products have seemingly-rectangular but *huge* footprints
    # (because they union many almost-indistinguishable footprints)
    # If they specify a resolution, we can simplify the geometry based on it.
    if footprint_expression is not None and dt.grid_spec and dt.grid_spec.resolution:
        resolution = min(abs(r) for r in dt.grid_spec.resolution)
        footprint_expression = func.ST_SimplifyPreserveTopology(
            footprint_expression, resolution / 4
        )

    # "expr == None" is valid in sqlalchemy:
    # pylint: disable=singleton-comparison
    time = md_type.dataset_fields["time"].alchemy_expression

    # Matches the logic in Dataset.center_time
    center_time = (func.lower(time) + (func.upper(time) - func.lower(time)) / 2).label(
        "center_time"
    )

    return [
        DATASET.c.id,
        DATASET.c.dataset_type_ref,
        center_time,
        (null() if footprint_expression is None else footprint_expression).label(
            "footprint"
        ),
        _region_code_field(dt).label("region_code"),
        _size_bytes_field(dt).label("size_bytes"),
        _dataset_creation_expression(md_type).label("creation_time"),
    ]


def _default_crs(dt: DatasetType) -> Optional[str]:
    storage = dt.definition.get("storage")
    if not storage:
        return None

    return storage.get("crs")


def _dataset_creation_expression(md: MetadataType) -> ClauseElement:
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
            return str(prefix + to_shape(o).wkt)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, PgRange):
            return ["∞" if o.lower_inf else o.lower, "∞" if o.upper_inf else o.upper]
        return repr(o)

    return json.dumps(obj, indent=4, default=fallback)


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
    def __init__(
        self, product: DatasetType, region_shapes: Dict[str, GeometryCollection]
    ) -> None:
        self.product = product
        self._region_shapes = region_shapes

    # Treated as an "id" in view code. What kind of region?
    name: str = "region"
    # A human-readable description displayed on a UI.
    description: str = "Regions"
    # Used when printing counts "1 region", "5 regions".
    unit_label: str = "region"
    units_label: str = "regions"

    @classmethod
    def for_product(
        cls, dt: DatasetType, region_shapes: Dict[str, GeometryCollection] = None
    ):
        region_code_field: Field = dt.metadata_type.dataset_fields.get("region_code")
        grid_spec = dt.grid_spec
        # Ingested grids trump the "region_code" field because they've probably sliced it up smaller.
        #
        # hltc has a grid spec, but most attributes are missing, so grid_spec functions fail.
        # Therefore: only assume there's a grid if tile_size is specified.
        if grid_spec is not None and grid_spec.tile_size:
            return GridRegionInfo(dt, region_shapes)
        elif region_code_field is not None:
            # Generic region info
            return RegionInfo(dt, region_shapes)
        elif "sat_path" in dt.metadata_type.dataset_fields:
            return SceneRegionInfo(dt, region_shapes)

        return None

    def geographic_extent(self, region_code: str) -> Optional[GeometryCollection]:
        return self._region_shapes.get(region_code)

    def geojson_extent(self, region_code):
        extent = self.geographic_extent(region_code)
        if not extent:
            return None
        return {
            "type": "Feature",
            "geometry": extent.__geo_interface__,
            "properties": {"region_code": region_code},
        }

    def dataset_region_code(self, dataset: Dataset) -> Optional[str]:
        """
        Get the region code for a dataset.

        This should always give the same result as the alchemy_expression() function,
        but is computed in pure python.

        Classes that override alchemy_expression should override this to match.
        """
        return dataset.metadata.region_code

    def alchemy_expression(self):
        """
        Get an alchemy expression that computes dataset's region code

        Classes that override this should also override dataset_region_code to match.
        """
        dt = self.product
        region_code_field: Field = dt.metadata_type.dataset_fields.get("region_code")
        # `alchemy_expression` is part of the postgres driver (PgDocField),
        # not the base Field class.
        if not hasattr(region_code_field, "alchemy_expression"):
            raise NotImplementedError(
                "ODC index driver doesn't support alchemy expressions"
            )
        return region_code_field.alchemy_expression

    def region_label(self, region_code: str) -> str:
        """
        Convert the region_code into something human-readable.
        """
        # Default plain, un-prettified.
        return region_code


class GridRegionInfo(RegionInfo):
    """Ingested datacube products have tiles"""

    name = "tiled"
    description = "Tiled product"
    unit_label = "tile"
    units_label = "tiles"

    def region_label(self, region_code: str) -> str:
        return "Tile {:+d}, {:+d}".format(*_from_xy_region_code(region_code))

    def alchemy_expression(self):
        """
        Get an sqlalchemy expression to calculate the region code (a string)

        This is usually the 'region_code' field, if one exists, but there are
        fallbacks for other native Satellites/Platforms.

        Eg.

        On Landsat scenes this is the path/row (separated by underscore)
        On tiles this is the tile numbers (separated by underscore: possibly with negative)
        On Sentinel this is MGRS number

        """
        dt = self.product
        grid_spec = dt.grid_spec

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

    def dataset_region_code(self, dataset: Dataset) -> Optional[str]:
        tiles = [
            tile
            for tile, _ in dataset.type.grid_spec.tiles(
                dataset.extent.centroid.boundingbox
            )
        ]
        if not len(tiles) == 1:
            raise ValueError(
                "Tiled dataset should only have one tile? "
                f"Got {tiles!r} for {dataset!r}"
            )
        x, y = tiles[0]
        return f"{x}_{y}"


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
    """Landsat WRS2"""

    name = "scenes"
    description = "Landsat WRS2 scene-based product"
    unit_label = "scene"
    units_label = "scenes"

    def region_label(self, region_code: str) -> str:
        if "_" in region_code:
            x, y = _from_xy_region_code(region_code)
            return f"Path {x}, Row {y}"
        else:
            return f"Path {region_code}"

    def alchemy_expression(self):
        dt = self.product
        # Generate region code for older sat_path/sat_row pairs.
        md_fields = dt.metadata_type.dataset_fields
        path_field: RangeDocField = md_fields["sat_path"]
        row_field: RangeDocField = md_fields["sat_row"]

        return case(
            [
                # Is this just one scene? Include it specifically
                (
                    row_field.lower.alchemy_expression
                    == row_field.greater.alchemy_expression,
                    func.concat(
                        path_field.lower.alchemy_expression.cast(String),
                        "_",
                        row_field.greater.alchemy_expression.cast(String),
                    ),
                ),
            ],
            # Otherwise it's a range of rows, so our region-code is the whole path.
            else_=path_field.lower.alchemy_expression.cast(String),
        )

    def dataset_region_code(self, dataset: Dataset) -> Optional[str]:
        path_range = dataset.metadata.fields["sat_path"]
        row_range = dataset.metadata.fields["sat_row"]
        if row_range is None and path_range is None:
            return None

        # If it's just one scene? Include it specifically
        if row_range[0] == row_range[1]:
            return f"{path_range[0]}_{row_range[1]}"
        # Otherwise it's a range of rows, so we say the whole path.
        else:
            return f"{path_range[0]}"


def _region_code_field(dt: DatasetType):
    """
    Get an sqlalchemy expression to calculate the region code (a string)
    """
    region_info = RegionInfo.for_product(
        dt,
        # The None is here bad OO design. The class probably should be split in two for different use-cases.
        None,
    )
    if region_info is not None:
        return region_info.alchemy_expression()
    else:
        _LOG.debug(
            "no_region_code",
            product_name=dt.name,
            metadata_type_name=dt.metadata_type.name,
        )
        return null()


def get_sample_dataset(*product_names: str, index: Index = None) -> Iterable[Dict]:
    with Datacube(index=index) as dc:
        index = dc.index
        for product_name in product_names:
            product = index.products.get_by_name(product_name)
            res = (
                alchemy_engine(index)
                .execute(
                    select(_select_dataset_extent_columns(product))
                    .where(
                        DATASET.c.dataset_type_ref
                        == bindparam("product_ref", product.id, type_=SmallInteger)
                    )
                    .where(DATASET.c.archived == None)
                    .limit(1)
                )
                .fetchone()
            )
            if res:
                yield dict(res)


@functools.lru_cache()
def _get_path_row_shapes():
    path_row_shapes = {}
    for shape_file in _WRS_PATH_ROW:
        with fiona.open(str(shape_file)) as f:
            for _k, item in f.items():
                prop = item["properties"]
                key = prop["PATH"], prop["ROW"]
                assert key not in path_row_shapes
                path_row_shapes[key] = shape(item["geometry"])
    return path_row_shapes


def get_mapped_crses(*product_names: str, index: Index = None) -> Iterable[Dict]:
    with Datacube(index=index) as dc:
        index = dc.index
        for product_name in product_names:
            product = index.products.get_by_name(product_name)

            # SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
            # pylint: disable=singleton-comparison
            res = (
                alchemy_engine(index)
                .execute(
                    select(
                        [
                            literal(product.name).label("product"),
                            get_dataset_srid_alchemy_expression(
                                product.metadata_type
                            ).label("crs"),
                        ]
                    )
                    .where(DATASET.c.dataset_type_ref == product.id)
                    .where(DATASET.c.archived == None)
                    .limit(1)
                )
                .fetchone()
            )
            if res:
                yield dict(res)


if __name__ == "__main__":
    print(
        _as_json(
            list(
                get_mapped_crses(
                    *(sys.argv[1:] or ["ls8_nbar_scene", "ls8_nbar_albers"])
                )
            )
        )
    )
    print(
        _as_json(
            list(
                get_sample_dataset(
                    *(sys.argv[1:] or ["ls8_nbar_scene", "ls8_nbar_albers"])
                )
            )
        )
    )
