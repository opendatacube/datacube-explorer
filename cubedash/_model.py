import os
import time
from pathlib import Path
from typing import Counter, Dict, Iterable, Optional, Tuple

import flask
import flask_themes
import structlog
from flask_caching import Cache
from shapely.geometry import MultiPolygon

# Fix up URL Scheme handling using this
# from https://stackoverflow.com/questions/23347387/x-forwarded-proto-and-flask
from werkzeug.middleware.proxy_fix import ProxyFix

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._extents import RegionInfo
from cubedash.summary._stores import ProductSummary
from datacube.index import index_connect
from datacube.model import DatasetType

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
app.jinja_options["extensions"].append("jinja2.ext.do")

app.config.setdefault("CACHE_TYPE", "null")
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
    return STORE.get(product_name, year, month, day)


def get_product_summary(product_name: str) -> ProductSummary:
    return STORE.get_product_summary(product_name)


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


@app.before_first_request
def enable_prometheus():
    # Enable deployment specific code for Prometheus metrics
    if os.environ.get("prometheus_multiproc_dir", False):
        from prometheus_flask_exporter.multiprocess import (
            GunicornInternalPrometheusMetrics,
        )

        metrics = GunicornInternalPrometheusMetrics(app)
        _LOG.info(f"Prometheus metrics enabled : {metrics}")


@app.before_first_request
def check_schema_compatibility():
    if not STORE.is_schema_compatible():
        raise RuntimeError(
            "Cubedash schema is out of date. "
            "Please rerun `cubedash-gen -v --init` to apply updates."
        )
