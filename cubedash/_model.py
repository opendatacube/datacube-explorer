import json
import os
import time
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

import flask
import sentry_sdk
import structlog
from datacube.index import index_connect
from datacube.model import Product
from flask_caching import Cache
from flask_cors import CORS
from flask_themer import Themer

# pylint: disable=import-error
from sentry_sdk.integrations.flask import FlaskIntegration
from shapely.geometry import MultiPolygon
from werkzeug.exceptions import HTTPException

# Fix up URL Scheme handling using this
# from https://stackoverflow.com/questions/23347387/x-forwarded-proto-and-flask
from werkzeug.middleware.proxy_fix import ProxyFix

from cubedash import _monitoring
from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._extents import RegionInfo
from cubedash.summary._stores import ProductSummary
from cubedash.summary._summarise import DEFAULT_TIMEZONE

from . import _utils as utils

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"

NAME = "cubedash"
BASE_DIR = Path(__file__).parent.parent


if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        environment=(
            os.getenv("SENTRY_ENV_TAG")
            if os.getenv("SENTRY_ENV_TAG")
            else "dev-explorer"
        ),
        integrations=[
            FlaskIntegration(),
        ],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
        # By default the SDK will try to use the SENTRY_RELEASE
        # environment variable, or infer a git commit
        # SHA as release, however you may want to set
        # something more human-readable.
        # release="myapp@1.0.0",
    )

cache = Cache()

DEFAULT_GROUPING_TIMEZONE = DEFAULT_TIMEZONE

# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking
# (hence validate=False).
STORE: SummaryStore = SummaryStore.create(
    index=index_connect(application_name=NAME, validate_connection=False),
    grouping_time_zone=DEFAULT_TIMEZONE,
)


def create_app(test_config=None):
    app = flask.Flask(NAME)

    # Also part of the fix from ^
    app.wsgi_app = ProxyFix(app.wsgi_app)

    # Optional environment settings file or variable
    if test_config is None:
        app.config.from_pyfile(BASE_DIR / "settings.env.py", silent=True)
        app.config.from_envvar("CUBEDASH_SETTINGS", silent=True)
    else:
        app.config.from_mapping(test_config)
    # Enable do template extension
    app.jinja_env.add_extension("jinja2.ext.do")

    app.config.setdefault("CACHE_TYPE", "NullCache")

    # Global defaults
    app.config.from_mapping(
        dict(
            CUBEDASH_DEFAULT_API_LIMIT=500,
            CUBEDASH_HARD_API_LIMIT=4000,
        )
    )

    cache.init_app(app=app, config=app.config)

    cors = (  # noqa: F841
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

    @app.errorhandler(500)
    def internal_server_error(error):
        return flask.render_template("500.html")

    @app.errorhandler(HTTPException)
    def handle_exception(e: HTTPException):
        return (
            utils.render(
                "message.html",
                title=e.code,
                message=e.description,
                e=e,
            ),
            e.code,
        )

    # Enable deployment specific code for Prometheus metrics
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR", False):
        from prometheus_flask_exporter.multiprocess import (
            GunicornInternalPrometheusMetrics,
        )

        metrics = GunicornInternalPrometheusMetrics(app, group_by="endpoint")
        _LOG.info("Prometheus metrics enabled : {metrics}", extra=dict(metrics=metrics))

    # Add server timings to http headers.
    if app.config.get("CUBEDASH_SHOW_PERF_TIMES", False):
        _monitoring.init_app_monitoring(app)

    with app.app_context():
        from . import (
            _api,
            _audit,
            _dataset,
            _filters,
            _pages,
            _platform,
            _product,
            _stac,
            _stac_legacy,
        )

        app.register_blueprint(_filters.bp)
        app.register_blueprint(_api.bp)
        app.register_blueprint(_dataset.bp)
        app.register_blueprint(_product.bp)
        app.register_blueprint(_platform.bp)
        app.register_blueprint(_audit.bp)
        app.register_blueprint(_stac.bp)
        app.register_blueprint(_stac_legacy.bp)
        app.register_blueprint(_pages.bp)

    return app


_LOG = structlog.get_logger()


@cache.memoize(timeout=60)
def get_time_summary(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
) -> TimePeriodOverview | None:
    return STORE.get(product_name, year, month, day)


@cache.memoize(timeout=60)
def get_time_summary_all_products() -> dict[tuple[str, int, int], int]:
    return STORE.get_all_dataset_counts()


def get_product_summary(product_name: str) -> ProductSummary:
    return STORE.get_product_summary(product_name)


ProductWithSummary = tuple[Product, ProductSummary | None]


@cache.memoize(timeout=120)
def get_products() -> Sequence[ProductWithSummary]:
    """
    The list of all products that we have generated reports for.
    """
    products = [
        (product, get_product_summary(product.name)) for product in STORE.all_products()
    ]
    if products and not STORE.list_complete_products():
        raise RuntimeError(
            "No products are summarised. Run `cubedash-gen --all` to generate some."
        )

    return products


@cache.memoize(timeout=120)
def get_products_with_summaries() -> list[ProductWithSummary]:
    """The list of products that we have generated summaries for."""
    return [
        (product, summary) for product, summary in get_products() if summary is not None
    ]


@cache.memoize(timeout=60)
def get_footprint_geojson(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
) -> dict | None:
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
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
) -> dict | None:
    product = STORE.get_product(product_name)

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


def _get_footprint(period: TimePeriodOverview) -> MultiPolygon | None:
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
) -> dict | None:
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
