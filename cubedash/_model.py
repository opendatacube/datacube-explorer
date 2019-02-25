import time
from functools import partial
from pathlib import Path
from typing import Counter, Dict, Iterable, Optional, Tuple

import dateutil.parser
import flask
import flask_themes
import pyproj
import shapely
import shapely.geometry
import shapely.ops
import shapely.prepared
import shapely.wkb
import structlog
from flask_caching import Cache
from shapely.geometry import MultiPolygon
from shapely.ops import transform

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._extents import RegionInfo
from cubedash.summary._stores import ProductSummary
from datacube.index import index_connect
from datacube.model import DatasetType

NAME = "cubedash"
BASE_DIR = Path(__file__).parent.parent

app = flask.Flask(NAME)

# Optional environment settings file or variable
app.config.from_pyfile(BASE_DIR / "settings.env.py", silent=True)
app.config.from_envvar("CUBEDASH_SETTINGS", silent=True)

app.config.setdefault("CACHE_TYPE", "simple")
cache = Cache(app=app, config=app.config)

app.config.setdefault("CUBEDASH_THEME", "odc")
flask_themes.setup_themes(app)

# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking
# (hence validate=False).
STORE: SummaryStore = SummaryStore.create(
    index_connect(application_name=NAME, validate_connection=False)
)

# Which product to show by default when loading '/'. Picks the first available.
DEFAULT_START_PAGE_PRODUCTS = app.config.get("CUBEDASH_DEFAULT_PRODUCTS") or (
    "ls7_nbar_scene",
    "ls5_nbar_scene",
)

_LOG = structlog.get_logger()


@cache.memoize(timeout=60)
def get_time_summary(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
) -> Optional[TimePeriodOverview]:
    # If it's a day, feel free to update/generate it, because it's quick.
    if day is not None:
        return STORE.get_or_update(product_name, year, month, day)

    return STORE.get(product_name, year, month, day)


def get_product_summary(product_name: str) -> ProductSummary:
    return STORE.get_product_summary(product_name)


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
    path = BASE_DIR / "generated.txt"
    if path.exists():
        date_text = path.read_text()
        try:
            return dateutil.parser.parse(date_text)
        except ValueError:
            _LOG.warn("invalid.summary.generated.txt", text=date_text, path=path)
    return STORE.get_last_updated()


ProductWithSummary = Tuple[DatasetType, ProductSummary]


@cache.memoize(timeout=120)
def get_products_with_summaries() -> Iterable[ProductWithSummary]:
    """
    The list of products that we have generated reports for.
    """
    index_products = {p.name: p for p in STORE.all_dataset_types()}
    products = [
        (index_products[product_name], get_product_summary(product_name))
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
    period = get_time_summary(product_name, year, month, day)
    if period is None:
        return None

    footprint = _get_footprint(period)
    if not footprint:
        return None

    return dict(
        type="Feature",
        geometry=footprint.__geo_interface__,
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

    product = STORE.get_dataset_type(product_name)

    region_info = RegionInfo.for_product(product)
    if not region_info:
        return None

    product_summary = STORE.get_product_summary(product.name)
    if not product_summary:
        # Valid product, but no summary generated.
        return None
    period = get_time_summary(product_name, year, month, day)
    if not period:
        # Valid product, but no summary generated.
        return None
    footprint_wrs84 = _get_footprint(period)

    start = time.time()
    regions = _get_regions_geojson(
        period.region_dataset_counts, footprint_wrs84, region_info
    )
    _LOG.debug("overview.region_gen", time_sec=time.time() - start)
    return regions


def _get_footprint(period: TimePeriodOverview):
    if not period or not period.dataset_count:
        return None

    if not period.footprint_geometry:
        return None
    start = time.time()
    tranform_wrs84 = partial(
        pyproj.transform,
        pyproj.Proj(init=period.footprint_crs),
        pyproj.Proj(init="epsg:4326"),
    )
    # It's possible to get self-intersection after transformation, presumably due to
    # rounding, so we buffer 0.
    footprint_wrs84 = transform(tranform_wrs84, period.footprint_geometry).buffer(0)
    _LOG.info(
        "overview.footprint_size_diff",
        from_len=len(period.footprint_geometry.wkt),
        to_len=len(footprint_wrs84.wkt),
    )
    _LOG.debug("overview.footprint_proj", time_sec=time.time() - start)

    return footprint_wrs84


def _get_regions_geojson(
    region_counts: Counter[str], footprint: MultiPolygon, region_info: RegionInfo
) -> Optional[Dict]:
    region_geometry = _region_geometry_function(region_info, footprint)
    if not region_geometry:
        # Regions are unsupported for product
        return None

    if region_counts:
        low, high = min(region_counts.values()), max(region_counts.values())
    else:
        low, high = 0, 0

    return {
        "type": "FeatureCollection",
        "properties": {
            "region_type": region_info.name,
            "region_unit_label": region_info.unit_label,
            "min_count": low,
            "max_count": high,
        },
        "features": [
            {
                "type": "Feature",
                "geometry": region_geometry(region_code).__geo_interface__,
                "properties": {
                    "region_code": region_code,
                    "label": region_info.region_label(region_code),
                    "count": region_counts[region_code],
                },
            }
            for region_code in (region_counts or [])
        ],
    }


def _region_geometry_function(region_info: RegionInfo, footprint):
    region_shape = region_info.geographic_extent

    if footprint is None:
        return region_shape
    else:
        footprint_boundary = shapely.prepared.prep(footprint.boundary)

        def region_geometry_cut(
            region_code: str
        ) -> shapely.geometry.GeometryCollection:
            """
            Cut the polygon down to the footprint
            """
            shapely_extent = region_shape(region_code)

            # We only need to cut up tiles that touch the edges of the footprint (including inner "holes")
            # Checking the boundary is ~2.5x faster than running intersection() blindly, from my tests.
            if footprint_boundary.intersects(shapely_extent):
                return footprint.intersection(shapely_extent)
            else:
                return shapely_extent

        return region_geometry_cut


@app.errorhandler(500)
def internal_server_error(error):
    args = {}
    if "sentry_event_id" in flask.g:
        args["sentry_event_id"] = flask.g.sentry_event_id

    return flask.render_template("500.html", **args)


# Optional Sentry error reporting. Add a SENTRY_CONFIG section to your config file to use it.
if "SENTRY_CONFIG" in app.config:
    # pylint: disable=import-error
    from raven.contrib.flask import Sentry
    from ._version import get_versions

    app.config["SENTRY_CONFIG"]["release"] = get_versions()["version"]
    SENTRY = Sentry(app)

    @app.context_processor
    def inject_sentry_info():
        # For Javascript error reporting. See the base template (base.html) and 500.html
        sentry_args = {"release": SENTRY.client.release}
        if SENTRY.client.environment:
            sentry_args["environment"] = SENTRY.client.environment

        return dict(
            sentry_public_dsn=SENTRY.client.get_public_dsn("https"),
            sentry_public_args=sentry_args,
        )
