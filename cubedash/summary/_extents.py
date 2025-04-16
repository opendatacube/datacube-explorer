import functools
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TypeAlias

import fiona
import structlog
from datacube.drivers.postgis._fields import PgDocField as PgisDocField
from datacube.drivers.postgis._fields import RangeDocField as PgisRangeDocField
from datacube.drivers.postgres._fields import PgDocField as PgresDocField
from datacube.drivers.postgres._fields import RangeDocField as PgresRangeDocField
from datacube.model import Dataset, Field, MetadataType, Product
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from shapely.geometry import shape
from sqlalchemy import (
    BigInteger,
    String,
    case,
    cast,
    func,
    null,
)
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql.elements import ClauseElement, Label
from sqlalchemy.types import TIMESTAMP
from typing_extensions import override

from cubedash._utils import (
    datetime_expression,
    expects_eo3_metadata_type,
    infer_crs,
    jsonb_doc_expression,
)
from cubedash.index import ExplorerIndex

_LOG = structlog.get_logger()

_WRS_PATH_ROW = [
    Path(__file__).parent.parent / "data" / "WRS2_descending" / "WRS2_descending.shp",
    Path(__file__).parent.parent / "data" / "WRS2_ascending" / "WRS2_acsending.shp",
]

PgDocField: TypeAlias = PgresDocField | PgisDocField
RangeDocField: TypeAlias = PgresRangeDocField | PgisRangeDocField


class UnsupportedWKTProductCRSError(NotImplementedError):
    """We can't, within Postgis, support arbitrary WKT CRSes at the moment."""

    def __init__(self, reason: str) -> None:
        self.reason = reason


def get_dataset_extent_alchemy_expression(
    e_index: ExplorerIndex, md: MetadataType, default_crs: str | None = None
):
    """
    Build an SQLAlchemy expression to get the extent for a dataset.

    It's returned as a postgis geometry.

    The logic here mirrors the extent() function of datacube.model.Dataset.
    """
    doc = jsonb_doc_expression(md)

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    projection_offset = _projection_doc_offset(md)

    if expects_eo3_metadata_type(md):
        return func.ST_SetSRID(
            case(
                # If we have geometry, use it as the polygon.
                (
                    doc[["geometry"]].is_not(None),
                    func.ST_GeomFromGeoJSON(doc[["geometry"]], type_=Geometry),
                ),
                # Otherwise construct a polygon from the computed bounds that ODC added on index.
                else_=_bounds_polygon(doc, projection_offset),
            ),
            get_dataset_srid_alchemy_expression(e_index, md, default_crs),
        )
    else:
        valid_data_offset = projection_offset + ["valid_data"]
        return func.ST_SetSRID(
            case(
                # If we have valid_data offset, use it as the polygon.
                (
                    doc[valid_data_offset].is_not(None),
                    func.ST_GeomFromGeoJSON(doc[valid_data_offset], type_=Geometry),
                ),
                # Otherwise construct a polygon from the four corner points.
                else_=_bounds_polygon(doc, projection_offset),
            ),
            get_dataset_srid_alchemy_expression(e_index, md, default_crs),
            type_=Geometry,
        )


def _projection_doc_offset(md):
    projection_offset = md.definition["dataset"]["grid_spatial"]
    return projection_offset


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


def _size_bytes_field(product: Product):
    md_fields = product.metadata_type.dataset_fields
    if "size_bytes" in md_fields:
        return md_fields["size_bytes"].alchemy_expression

    return jsonb_doc_expression(product.metadata_type)["size_bytes"].astext.cast(
        BigInteger
    )


def get_dataset_srid_alchemy_expression(
    e_index: ExplorerIndex, md: MetadataType, default_crs: str | None = None
):
    doc = jsonb_doc_expression(md)

    if "grid_spatial" not in md.definition["dataset"]:
        # Non-spatial product
        return None

    doc_projection = doc[_projection_doc_offset(md)]

    if expects_eo3_metadata_type(md):
        spatial_ref = doc[["crs"]].astext
    else:
        # Most have a spatial_reference field we can use directly.
        # spatial_ref = doc[projection_offset + ["spatial_reference"]].astext
        spatial_ref = doc_projection["spatial_reference"].astext

    # When datasets have no CRS, optionally use this as default.
    # default_crs_expression = None
    if default_crs:
        # can this be replaced with odc-geo logic?
        if not default_crs.lower().startswith(
            "epsg:"
        ) and not default_crs.lower().startswith("esri:"):
            # HACK: Change default CRS with inference
            inferred_crs = infer_crs(default_crs)
            if inferred_crs is None:
                raise UnsupportedWKTProductCRSError(
                    f"WKT Product CRSes are not currently well supported, and "
                    f"we can't infer this product's one. "
                    f"(Ideally use an auth-name format for CRS, such as 'EPSG:1234') "
                    f"Got: {default_crs!r}"
                )
            default_crs = inferred_crs

    return e_index.ds_srid_expression(spatial_ref, doc_projection, default_crs)


def _gis_point(doc, doc_offset):
    return func.ST_MakePoint(
        doc[doc_offset + ["x"]].astext.cast(postgres.DOUBLE_PRECISION),
        doc[doc_offset + ["y"]].astext.cast(postgres.DOUBLE_PRECISION),
    )


def refresh_spatial_extents(
    e_index: ExplorerIndex,
    product: Product,
    clean_up_deleted=False,
    assume_after_date: datetime | None = None,
):
    """
    Update the spatial extents to match any changes upstream in ODC.

    :param assume_after_date: Only scan datasets that have changed after the given (db server) time.
                              If None, all datasets will be regenerated.
    :param clean_up_deleted: Scan for any manually deleted rows too. Slow.
    """

    log = _LOG.bind(product_name=product.name, after_date=assume_after_date)

    log.info(
        "spatial_archival",
    )
    # First, remove any archived datasets from our spatial table.
    changed = e_index.delete_datasets(product.id, assume_after_date)

    log.info(
        "spatial_archival.end",
        change_count=changed,
    )

    # Forcing? Check every other dataset for removal, so we catch manually-deleted rows from the table.
    if clean_up_deleted:
        log.warning(
            "spatial_deletion_full_scan",
        )
        changed += e_index.delete_datasets(product.id, full=True)
        log.info(
            "spatial_deletion_scan.end",
            change_count=changed,
        )

    # We'll update first, then insert new records.
    # -> We do it in this order so that inserted records aren't immediately updated.
    # (Note: why don't we do this in one upsert? Because we get our sqlalchemy expressions
    #        through ODC's APIs and can't choose alternative table aliases to make sub-queries.
    #        Maybe you can figure out a workaround, though?)
    # >> presumably fixed in the upsert_datasets function?
    column_values = {
        c.name: c for c in _select_dataset_extent_columns(e_index, product)
    }

    if assume_after_date is None:
        log.warning("spatial_update.recreating_everything")

    # Update any changed datasets
    log.info(
        "spatial_upsert",
        product_name=product.name,
        after_date=assume_after_date,
    )

    changed += e_index.upsert_datasets(product.id, column_values, assume_after_date)
    log.info("spatial_upsert.end", product_name=product.name, change_count=changed)

    # If we changed data...
    if changed:
        # And it's a non-spatial product...
        if (
            get_dataset_extent_alchemy_expression(e_index, product.metadata_type)
            is None
        ):
            # And it has WRS path/rows...
            if "sat_path" in product.metadata_type.dataset_fields:
                # We can synthesize the polygons!
                log.info(
                    "spatial_synthesizing",
                )
                shapes = _get_path_row_shapes()
                rows = [
                    row
                    for row in e_index.ds_search_returning(
                        ("id", "sat_path", "sat_row"), args={"product": product.name}
                    )
                    if row.sat_path.lower is not None
                ]
                if rows:
                    e_index.synthesize_dataset_footprint(rows, shapes)
            log.info(
                "spatial_synthesizing.end",
            )

    return changed


def _select_dataset_extent_columns(
    e_index: ExplorerIndex, product: Product
) -> List[Label]:
    """
    Get columns for all fields which go into the spatial table
    for this Product.
    """
    md_type = product.metadata_type
    # If this product has lat/lon fields, we can take spatial bounds.

    footprint_expression = get_dataset_extent_alchemy_expression(
        e_index, md_type, default_crs=_default_crs(product)
    )

    # Some time-series-derived products have seemingly-rectangular but *huge* footprints
    # (because they union many almost-indistinguishable footprints)
    # If they specify a resolution, we can simplify the geometry based on it.
    if (
        footprint_expression is not None
        and product.grid_spec
        and product.grid_spec.resolution
    ):
        resolution = min(abs(r) for r in product.grid_spec.resolution)
        footprint_expression = func.ST_SimplifyPreserveTopology(
            footprint_expression, resolution / 4
        )

    return [
        datetime_expression(md_type),
        (null() if footprint_expression is None else footprint_expression).label(
            "footprint"
        ),
        _region_code_field(product).label("region_code"),
        _size_bytes_field(product).label("size_bytes"),
        _dataset_creation_expression(md_type).label("creation_time"),
    ]


def _default_crs(product: Product) -> Optional[str]:
    storage = product.definition.get("storage")
    if not storage:
        return None

    return storage.get("crs")


def _dataset_creation_expression(md: MetadataType) -> ClauseElement:
    """SQLAlchemy expression for the creation (processing) time of a dataset"""

    # Either there's a field called "created", or we fallback to the default "creation_dt' in metadata type.
    created_field = md.dataset_fields.get("created")
    if created_field is not None:
        assert isinstance(created_field, PgDocField)
        creation_expression = created_field.alchemy_expression
    else:
        creation_expression = md.dataset_fields.get("creation_time").alchemy_expression

    # If they're missing a dataset-creation time, fall back to the time it was indexed.
    return func.coalesce(
        cast(creation_expression, TIMESTAMP(timezone=True)),
        md.dataset_fields.get("indexed_time").alchemy_expression,
    )


# not used anywhere?
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
        return repr(o)

    return json.dumps(obj, indent=4, default=fallback)


@dataclass
class RegionSummary:
    product_name: str
    region_code: str
    count: int
    generation_time: datetime
    footprint_wgs84: Geometry

    @property
    def footprint_geojson(self):
        extent = self.footprint_wgs84
        if not extent:
            return None
        return {
            "type": "Feature",
            "geometry": extent.__geo_interface__,
            "properties": {"region_code": self.region_code, "count": self.count},
        }


@dataclass
class ProductArrival:
    """What arrived for a given product on a particular day?"""

    product_name: str
    day: date
    # Count of datasets added on the given day.
    dataset_count: int

    # A few dataset ids among the arrivals
    sample_dataset_ids: List[uuid.UUID]


class RegionInfo:
    def __init__(
        self, product: Product, known_regions: Optional[Dict[str, RegionSummary]]
    ) -> None:
        self.product = product
        self._known_regions = known_regions

    # Treated as an "id" in view code. What kind of region?
    name: str = "region"
    # A human-readable description displayed on a UI.
    description: str = "Regions"
    # Used when printing counts "1 region", "5 regions".
    unit_label: str = "region"
    units_label: str = "regions"

    @classmethod
    def for_product(
        cls, product: Product, known_regions: Dict[str, RegionSummary] | None = None
    ):
        region_code_field: Field = product.metadata_type.dataset_fields.get(
            "region_code"
        )

        grid_spec = product.grid_spec
        # Ingested grids trump the "region_code" field because they've probably sliced it up smaller.
        #
        # hltc has a grid spec, but most attributes are missing, so grid_spec functions fail.
        # Therefore: only assume there's a grid if tile_size is specified.
        if region_code_field is not None:
            # Generic region info
            return RegionInfo(product, known_regions)
        elif grid_spec is not None and grid_spec.tile_size:
            return GridRegionInfo(product, known_regions)
        elif "sat_path" in product.metadata_type.dataset_fields:
            return SceneRegionInfo(product, known_regions)

        return None

    def region(self, region_code: str) -> Optional[RegionSummary]:
        return self._known_regions.get(region_code)

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
        product = self.product
        region_code_field: Field = product.metadata_type.dataset_fields.get(
            "region_code"
        )
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

    @override
    def region_label(self, region_code: str) -> str:
        return "Tile {:+d}, {:+d}".format(*_from_xy_region_code(region_code))

    @override
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
        product = self.product
        grid_spec = product.grid_spec

        doc = jsonb_doc_expression(product.metadata_type)
        projection_offset = _projection_doc_offset(product.metadata_type)
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

    @override
    def dataset_region_code(self, dataset: Dataset) -> Optional[str]:
        tiles = [
            tile
            for tile, _ in dataset.product.grid_spec.tiles(
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

    @override
    def region_label(self, region_code: str) -> str:
        if "_" in region_code:
            x, y = _from_xy_region_code(region_code)
            return f"Path {x}, Row {y}"
        else:
            return f"Path {region_code}"

    @override
    def alchemy_expression(self):
        product = self.product
        # Generate region code for older sat_path/sat_row pairs.
        md_fields = product.metadata_type.dataset_fields
        path_field: RangeDocField = md_fields["sat_path"]
        row_field: RangeDocField = md_fields["sat_row"]

        return case(
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
            # Otherwise it's a range of rows, so our region-code is the whole path.
            else_=path_field.lower.alchemy_expression.cast(String),
        )

    @override
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


def _region_code_field(product: Product):
    """
    Get an sqlalchemy expression to calculate the region code (a string)
    """
    region_info = RegionInfo.for_product(
        product,
        # The None is here bad OO design. The class probably should be split in two for different use-cases.
        None,
    )
    if region_info is not None:
        return region_info.alchemy_expression()
    else:
        _LOG.debug(
            "no_region_code",
            product_name=product.name,
            metadata_type_name=product.metadata_type.name,
        )
        return null()


# would be able to replace with index.datasets.search_returning except that there's no way for us to specify the
# alchemy expressions for the columns that don't exist in the core tables
def get_sample_dataset(products, e_index: ExplorerIndex) -> Iterable[Dict]:
    for product in products:
        res = e_index.sample_dataset(
            product.id, _select_dataset_extent_columns(e_index, product)
        ).fetchone()
        if res:
            yield dict(res._mapping)


@functools.lru_cache
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


# see comment on get_sample_dataset
def get_mapped_crses(products, e_index: ExplorerIndex) -> Iterable[Dict]:
    for product in products:
        res = e_index.mapped_crses(
            product,
            get_dataset_srid_alchemy_expression(e_index, product.metadata_type).label(
                "crs"
            ),
        ).fetchone()
        if res:
            yield dict(res._mapping)
