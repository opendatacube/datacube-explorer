import json
import os
import time
from pathlib import Path
from typing import Counter, Dict, List, Optional, Tuple

import flask
import structlog
from datacube.index import index_connect
from datacube.model import DatasetType
from flask_caching import Cache
from flask_cors import CORS
from flask_themer import Themer
from shapely.geometry import MultiPolygon

# Fix up URL Scheme handling using this
# from https://stackoverflow.com/questions/23347387/x-forwarded-proto-and-flask
from werkzeug.middleware.proxy_fix import ProxyFix

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._extents import RegionInfo
from cubedash.summary._stores import ProductSummary

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"

NAME = "cubedash"
BASE_DIR = Path(__file__).parent.parent

app = flask.Flask(NAME)
# Also part of the fix from ^
app.wsgi_app = ProxyFix(app.wsgi_app)

# Optional environment settings file or variable
app.config.from_pyfile(BASE_DIR / "settings.env.py", silent=True)
app.config.from_envvar("CUBEDASH_SETTINGS", silent=True)

# Enable do template extension
app.jinja_env.add_extension("jinja2.ext.do")

app.config.setdefault("CACHE_TYPE", "null")

# Global defaults
app.config.from_mapping(
    dict(
        CUBEDASH_DEFAULT_API_LIMIT=500,
        CUBEDASH_HARD_API_LIMIT=4000,
    )
)

cache = Cache(app=app, config=app.config)

cors = (
    CORS(app, resources=[r"/stac/*", r"/api/*"])
    if app.config.get("CUBEDASH_CORS", True)
    else None
)

app.config.setdefault("CUBEDASH_THEME", "odc")
themer = Themer(app)


@themer.current_theme_loader
def get_current_theme():
    return app.config["CUBEDASH_THEME"]


# The theme can set its own default config options.
with (Path(app.root_path) / "themes" / themer.current_theme / "info.json").open(
    "r"
) as f:
    for key, value in json.load(f)["defaults"].items():
        app.config.setdefault(key, value)

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
    return STORE.get(product_name, year, month, day)


@cache.memoize(timeout=60)
def get_time_summary_all_products() -> Dict[Tuple[str, int, int], int]:
    return STORE.get_all_dataset_counts()


def get_product_summary(product_name: str) -> ProductSummary:
    return STORE.get_product_summary(product_name)


ProductWithSummary = Tuple[DatasetType, Optional[ProductSummary]]


@cache.memoize(timeout=120)
def get_products() -> List[ProductWithSummary]:
    """
    The list of all products that we have generated reports for.
    """
    products = [
        (product, get_product_summary(product.name))
        for product in STORE.all_dataset_types()
    ]
    if products and not STORE.list_complete_products():
        raise RuntimeError(
            "No products are summarised. " "Run `cubedash-gen --all` to generate some."
        )

    return products


@cache.memoize(timeout=120)
def get_products_with_summaries() -> List[ProductWithSummary]:
    """The list of products that we have generated summaries for."""
    return [
        (product, summary) for product, summary in get_products() if summary is not None
    ]


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

    region_info = STORE.get_product_region_info(product_name)
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

    start = time.time()
    region_counts = period.region_dataset_counts
    if region_counts is None:
        return None

    # If all datasets have no region name, don't bother showing regions.
    #
    # (datasets that are missing a region are in the None region)
    if len(region_counts) == 1 and list(region_counts.keys()) == [None]:
        return None

    regions = _get_regions_geojson(region_counts, region_info)
    _LOG.debug("overview.region_gen", time_sec=time.time() - start)
    return regions


def _get_footprint(period: TimePeriodOverview) -> Optional[MultiPolygon]:
    if not period or not period.dataset_count:
        return None

    if not period.footprint_geometry:
        return None
    start = time.time()
    footprint_wgs84 = period.footprint_wgs84
    _LOG.info(
        "overview.footprint_size_diff",
        from_len=len(period.footprint_geometry.wkt),
        to_len=len(footprint_wgs84.wkt),
    )
    _LOG.debug("overview.footprint_proj", time_sec=time.time() - start)

    return footprint_wgs84


def _get_regions_geojson(
    region_counts: Counter[str], region_info: RegionInfo
) -> Optional[Dict]:
    if not region_info:
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
                "geometry": region_info.region(
                    region_code
                ).footprint_wgs84.__geo_interface__,
                "properties": {
                    "region_code": region_code,
                    "label": region_info.region_label(region_code),
                    "count": region_counts[region_code],
                },
            }
            for region_code in (region_counts or [])
            if region_info.region(region_code) is not None
        ],
    }


@app.errorhandler(500)
def internal_server_error(error):
    args = {}
    if "sentry_event_id" in flask.g:
        args["sentry_event_id"] = flask.g.sentry_event_id

    return flask.render_template("500.html", **args)


# Optional Sentry error reporting. Add a SENTRY_CONFIG section to your config file to use it.
# This is injected before application starts serving requests
@app.before_first_request
def enable_sentry():
    if "SENTRY_CONFIG" in app.config:
        # pylint: disable=import-error
        from raven.contrib.flask import Sentry

        app.config["SENTRY_CONFIG"]["release"] = __version__
        sentry = Sentry(app)

        @app.context_processor
        def inject_sentry_info():
            # For Javascript error reporting. See the base template (base.html) and 500.html
            sentry_args = {"release": sentry.client.release}
            if sentry.client.environment:
                sentry_args["environment"] = sentry.client.environment

            return dict(
                sentry_public_dsn=sentry.client.get_public_dsn("https"),
                sentry_public_args=sentry_args,
            )


@app.before_first_request
def enable_prometheus():
    # Enable deployment specific code for Prometheus metrics
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR", False):
        from prometheus_flask_exporter.multiprocess import (
            GunicornInternalPrometheusMetrics,
        )

        metrics = GunicornInternalPrometheusMetrics(app, group_by="endpoint")
        _LOG.info(f"Prometheus metrics enabled : {metrics}")


@app.before_first_request
def check_schema_compatibility():
    if not STORE.is_schema_compatible():
        raise RuntimeError(
            "Cubedash schema is out of date. "
            "Please rerun `cubedash-gen -v --init` to apply updates."
        )
