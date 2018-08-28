from __future__ import absolute_import

import functools
import time
from pathlib import Path
from typing import Counter, Dict, Iterable, Optional, Tuple

import dateutil.parser
import fiona
import flask
import pyproj
import shapely
import shapely.geometry
import shapely.ops
import shapely.prepared
import shapely.wkb
import structlog
from flask_caching import Cache
from shapely.geometry import MultiPolygon, shape
from shapely.ops import transform

from cubedash.summary import SummaryStore, TimePeriodOverview
from datacube.index import index_connect
from datacube.model import DatasetType

NAME = "cubedash"

app = flask.Flask(NAME)
cache = Cache(app=app, config={"CACHE_TYPE": "simple"})

# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking
# (hence validate=False).
STORE = SummaryStore.create(
    index_connect(application_name=NAME, validate_connection=False)
)

# Pre-computed summaries of products (to avoid doing them on page load).
SUMMARIES_DIR = Path(__file__).parent.parent / "product-summaries"

# Which product to show by default when loading '/'. Picks the first available.
DEFAULT_START_PAGE_PRODUCTS = ("ls7_nbar_scene", "ls5_nbar_scene")

_LOG = structlog.get_logger()

_WRS_PATH_ROW = (
    Path(__file__).parent / "data" / "WRS2_descending" / "WRS2_descending.shp"
)


@cache.memoize(timeout=60)
def get_summary(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
) -> Optional[TimePeriodOverview]:
    # If it's a day, feel free to update/generate it, because it's quick.
    if day is not None:
        return STORE.get_or_update(product_name, year, month, day)

    return STORE.get(product_name, year, month, day)


@cache.memoize(timeout=60)
def get_datasets_geojson(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    limit: int = 500,
) -> Dict:
    return STORE.get_dataset_footprints(product_name, year, month, day, limit=limit)


@cache.memoize(timeout=120)
def get_last_updated():
    # Drop a text file in to override the "updated time": for example, when we know it's an old clone of our DB.
    path = SUMMARIES_DIR / "generated.txt"
    if path.exists():
        date_text = path.read_text()
        try:
            return dateutil.parser.parse(date_text)
        except ValueError:
            _LOG.warn("invalid.summary.generated.txt", text=date_text, path=path)
    return STORE.get_last_updated()


@cache.memoize(timeout=120)
def get_products_with_summaries() -> Iterable[Tuple[DatasetType, TimePeriodOverview]]:
    """
    The list of products that we have generated reports for.
    """
    index_products = {p.name: p for p in STORE.index.products.get_all()}
    products = [
        (index_products[product_name], get_summary(product_name))
        for product_name in STORE.list_complete_products()
    ]
    if not products:
        raise RuntimeError(
            "No product reports. "
            "Run `python -m cubedash.generate --all` to generate some."
        )

    return products


@cache.memoize(timeout=60)
def get_footprint_geojson(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
) -> Optional[Dict]:
    period = get_summary(product_name, year, month, day)
    if period is None:
        return None

    return dict(
        type="Feature",
        geometry=_get_footprint(period).__geo_interface__,
        properties=dict(
            dataset_count=period.footprint_count,
            product_name=product_name,
            time_spec=[year, month, day],
        ),
    )


@cache.memoize(timeout=60)
def get_regions_geojson(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
) -> Optional[Dict]:
    period = get_summary(product_name, year, month, day)
    if period is None:
        return None

    product = STORE.index.products.get_by_name(product_name)
    if product is None:
        raise RuntimeError("Unknown product despite having a summary?", product_name)

    # The per-dataset view is less useful now that we show grids separately.
    # datasets = None if selected_summary.dataset_count > 1000
    # else get_datasets_geojson(product_name, year, month, day)

    if not period.region_dataset_counts:
        return None

    footprint_wrs84 = _get_footprint(period)

    start = time.time()
    regions = _get_regions_geojson(
        period.region_dataset_counts, footprint_wrs84, product
    )
    _LOG.debug("overview.region_gen", time_sec=time.time() - start)

    return regions


def _get_footprint(period: TimePeriodOverview):
    if not period or not period.dataset_count:
        return None

    if not period.footprint_geometry:
        return None

    start = time.time()
    from_crs = pyproj.Proj(init=period.footprint_crs)
    to_crs = pyproj.Proj(init="epsg:4326")
    footprint_wrs84 = transform(
        lambda x, y: pyproj.transform(from_crs, to_crs, x, y), period.footprint_geometry
    )
    _LOG.info(
        "overview.footprint_size_diff",
        from_len=len(period.footprint_geometry.wkt),
        to_len=len(footprint_wrs84.wkt),
    )
    _LOG.debug("overview.footprint_proj", time_sec=time.time() - start)

    return footprint_wrs84


def _get_regions_geojson(
    region_counts: Counter[str], footprint: MultiPolygon, product: DatasetType
) -> Optional[Dict]:
    region_geometry = _region_geometry_function(product, footprint)
    if not region_geometry:
        return None

    low, high = min(region_counts.values()), max(region_counts.values())
    return {
        "type": "FeatureCollection",
        "properties": {"region_item_name": "Tile", "min_count": low, "max_count": high},
        "features": [
            {
                "type": "Feature",
                "geometry": region_geometry(region_code).__geo_interface__,
                "properties": {
                    "region_code": region_code,
                    "count": region_counts[region_code],
                },
            }
            for region_code in region_counts
        ],
    }


def _from_xy_region_code(region_code: str):
    x, y = region_code.split("_")
    return int(x), int(y)


def _region_geometry_function(product, footprint):
    grid_spec = product.grid_spec
    md_fields = product.metadata_type.dataset_fields
    # TODO: Geometry for other types of regions (path/row, MGRS)

    # hltc has a grid spec, but most attributes are missing, so grid_spec functions fail.
    # Therefore: only assume there's a grid if tile_size is specified. TODO: Is the product wrong?
    if grid_spec and grid_spec.tile_size:

        def region_geometry(region_code: str) -> shapely.geometry.GeometryCollection:
            """
            Get a whole polygon for a gridcell
            """
            extent = grid_spec.tile_geobox(
                _from_xy_region_code(region_code)
            ).geographic_extent
            # TODO: The ODC Geometry __geo_interface__ breaks for some products
            # (eg, when the inner type is a GeometryCollection?)
            # So we're now converting to shapely to do it.
            # TODO: Is there a nicer way to do this?
            # pylint: disable=protected-access
            shapely_extent = shapely.wkb.loads(extent._geom.ExportToWkb())

            return shapely_extent

    elif "sat_path" in md_fields:
        path_row_shapes = _get_path_row_shapes()

        def region_geometry(region_code: str) -> shapely.geometry.GeometryCollection:
            return path_row_shapes[_from_xy_region_code(region_code)]

    else:
        _LOG.info("region.geom.unknown", product_name=product.name)
        return None

    if footprint is None:
        return region_geometry
    else:
        footprint_boundary = shapely.prepared.prep(footprint.boundary)

        def region_geometry_cut(
            region_code: str
        ) -> shapely.geometry.GeometryCollection:
            """
            Cut the polygon down to the footprint
            """
            shapely_extent = region_geometry(region_code)

            # We only need to cut up tiles that touch the edges of the footprint (including inner "holes")
            # Checking the boundary is ~2.5x faster than running intersection() blindly, from my tests.
            if footprint_boundary.intersects(shapely_extent):
                return footprint.intersection(shapely_extent)
            else:
                return shapely_extent

        return region_geometry_cut


@functools.lru_cache()
def _get_path_row_shapes():
    path_row_shapes = {}
    with fiona.open(str(_WRS_PATH_ROW)) as f:
        for k, item in f.items():
            prop = item["properties"]
            path_row_shapes[prop["PATH"], prop["ROW"]] = shape(item["geometry"])
    return path_row_shapes
