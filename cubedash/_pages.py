import itertools
import werkzeug
from datetime import datetime, timedelta
from typing import List, Tuple

import flask
import structlog
from flask import Response, abort, redirect, request, url_for

from werkzeug.datastructures import MultiDict
werkzeug.cached_property = werkzeug.utils.cached_property

import cubedash
import datacube
from cubedash import _audit, _monitoring
from cubedash._model import ProductWithSummary
from cubedash.summary import RegionInfo, TimePeriodOverview
from cubedash.summary._stores import ProductSummary
from datacube.model import DatasetType, Range
from datacube.scripts.dataset import build_dataset_info

from . import _api, _dataset, _filters, _model, _platform, _product, _reports, _stac
from . import _utils as utils
from ._utils import as_rich_json

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_product.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_reports.bp)
app.register_blueprint(_audit.bp)
app.register_blueprint(_stac.bp)

_LOG = structlog.getLogger()

_HARD_SEARCH_LIMIT = app.config.get("CUBEDASH_HARD_SEARCH_LIMIT", 150)

# Add server timings to http headers.
if app.config.get("CUBEDASH_SHOW_PERF_TIMES", False):
    _monitoring.init_app_monitoring()


# @app.route('/')
@app.route("/<product_name>")
@app.route("/<product_name>/<int:year>")
@app.route("/<product_name>/<int:year>/<int:month>")
@app.route("/<product_name>/<int:year>/<int:month>/<int:day>")
def overview_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    product, product_summary, selected_summary = _load_product(
        product_name, year, month, day
    )
    return utils.render(
        "overview.html",
        year=year,
        month=month,
        day=day,
        # Which data to preload with the page?
        regions_geojson=_model.get_regions_geojson(product_name, year, month, day),
        datasets_geojson=None,  # _model.get_datasets_geojson(product_name, year, month, day),
        footprint_geojson=_model.get_footprint_geojson(product_name, year, month, day),
        product=product,
        product_region_info=RegionInfo.for_product(product),
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
    )


# @app.route('/datasets')
@app.route("/datasets/<product_name>")
@app.route("/datasets/<product_name>/<int:year>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def search_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    product, product_summary, selected_summary = _load_product(
        product_name, year, month, day
    )
    time_range = utils.as_time_range(
        year, month, day, tzinfo=_model.STORE.grouping_timezone
    )

    args = MultiDict(flask.request.args)
    query = utils.query_to_search(args, product=product)

    # Always add time range, selected product to query
    if product_name:
        query["product"] = product_name

    if "time" in query:
        # If they left one end of the range open, fill it in with the product bounds.
        search_time = query["time"]
        assert isinstance(search_time, Range)
        if product_summary:
            query["time"] = Range(
                search_time.begin or product_summary.time_earliest,
                search_time.end or product_summary.time_latest + timedelta(days=1),
            )
    # The URL time range always trumps args.
    if time_range:
        query["time"] = time_range

    _LOG.info("query", query=query)

    # TODO: Add sort option to index API
    datasets = sorted(
        _model.STORE.index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
        key=lambda d: d.center_time,
    )

    if request_wants_json():
        return as_rich_json(
            dict(datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets])
        )

    # For display on the page (and future searches).
    if "time" not in query and product_summary:
        query["time"] = Range(
            product_summary.time_earliest,
            product_summary.time_latest + timedelta(days=1),
        )

    return utils.render(
        "search.html",
        year=year,
        month=month,
        day=day,
        product=product,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
        datasets=datasets,
        query_params=query,
        result_limit=_HARD_SEARCH_LIMIT,
    )


@app.route("/region/<product_name>/<region_code>")
@app.route("/region/<product_name>/<region_code>/<int:year>")
@app.route("/region/<product_name>/<region_code>/<int:year>/<int:month>")
@app.route("/region/<product_name>/<region_code>/<int:year>/<int:month>/<int:day>")
def region_page(
    product_name: str = None,
    region_code: str = None,
    year: int = None,
    month: int = None,
    day: int = None,
):
    product, product_summary, selected_summary = _load_product(
        product_name, year, month, day
    )

    region_info = RegionInfo.for_product(product)
    if not region_info:
        abort(404, f"Product {product_name} has no region specification.")

    datasets = list(
        _model.STORE.find_datasets_for_region(
            product_name, region_code, year, month, day, limit=_HARD_SEARCH_LIMIT
        )
    )

    if len(datasets) == 1 and "feelinglucky" in flask.request.args:
        return flask.redirect(url_for("dataset.dataset_page", id_=datasets[0].id))

    if request_wants_json():
        return as_rich_json(
            dict(datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets])
        )

    return utils.render(
        "region.html",
        year=year,
        month=month,
        day=day,
        region_code=region_code,
        product=product,
        product_region_info=region_info,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
        datasets=datasets,
        result_limit=_HARD_SEARCH_LIMIT,
    )


@app.route("/<product_name>/spatial")
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


@app.route("/<product_name>/timeline")
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


def _load_product(
    product_name, year, month, day
) -> Tuple[DatasetType, ProductSummary, TimePeriodOverview]:
    product = None
    if product_name:
        try:
            product = _model.STORE.get_dataset_type(product_name)
        except KeyError:
            abort(404, "Unknown product %r" % product_name)

    product_summary = _model.get_product_summary(product_name)
    time_summary = _model.get_time_summary(product_name, year, month, day)
    return product, product_summary, time_summary


def request_wants_json():
    best = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return (
        best == "application/json"
        and request.accept_mimetypes[best] > request.accept_mimetypes["text/html"]
    )


@app.route("/about")
def about_page():
    return utils.render("about.html")


@app.context_processor
def inject_globals():
    last_updated = _model.get_last_updated()
    # If there's no global data refresh time, show the time the current product was summarised.
    if not last_updated and "product_name" in flask.request.view_args:
        product_summary = _model.STORE.get_product_summary(
            flask.request.view_args["product_name"]
        )
        if product_summary:
            last_updated = datetime.now() - product_summary.last_refresh_age

    return dict(
        grouped_products=_get_grouped_products(),
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
        app_version=cubedash.__version__,
        grouping_timezone=_model.STORE.grouping_timezone,
        last_updated_time=last_updated,
    )


def _get_grouped_products() -> List[Tuple[str, List[ProductWithSummary]]]:
    """
    We group products using the configured grouping field (default "product_type").

    Anything left ungrouped will be placed at the end in groups of
    configurable max size.
    """
    product_summaries = _model.get_products_with_summaries()
    # Which field should we use when grouping products in the top menu?
    group_by_field = app.config.get("CUBEDASH_PRODUCT_GROUP_BY_FIELD", "product_type")
    group_field_size = app.config.get("CUBEDASH_PRODUCT_GROUP_SIZE", 5)

    # Group using the configured key, or fall back to the product name.
    def key(t):
        return t[0].fields.get(group_by_field) or t[0].name

    grouped_product_summarise = sorted(
        (
            (name or "", list(items))
            for (name, items) in itertools.groupby(
                sorted(product_summaries, key=key), key=key
            )
        ),
        # Show largest groups first
        key=lambda k: len(k[1]),
        reverse=True,
    )
    return _merge_singular_groups(grouped_product_summarise, group_field_size)


def _merge_singular_groups(
    grouped_product_summarise: List[Tuple[str, List[ProductWithSummary]]],
    remainder_group_size=5,
) -> List[Tuple[str, List[ProductWithSummary]]]:
    """
    Remove groups with only one member, and place them at the end in batches.
    """
    lonely_products = []
    for group, items in reversed(grouped_product_summarise):
        if len(items) > 1:
            break
        lonely_products.extend(items)
        grouped_product_summarise = grouped_product_summarise[:-1]

    there_are_groups = len(grouped_product_summarise) > 0

    lonely_products = sorted(lonely_products, key=lambda p: p[1].name)
    for i, lonely_group in enumerate(chunks(lonely_products, remainder_group_size)):
        group_name = ""
        if i == 0:
            group_name = "Other Products" if there_are_groups else "Products"
        grouped_product_summarise.append((group_name, lonely_group))
    return grouped_product_summarise


def chunks(l: List, n: int):
    """
    Split list into chunks of max size n.

    >>> list(chunks([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    >>> list(chunks([1, 2, 3, 4, 5, 6, 7, 8], 3))
    [[1, 2, 3], [4, 5, 6], [7, 8]]
    >>> list(chunks([1, 2, 3], 3))
    [[1, 2, 3]]
    >>> list(chunks([1, 2, 3], 4))
    [[1, 2, 3]]
    >>> list(chunks([], 3))
    []
    """
    for i in range(0, len(l), n):
        yield l[i : i + n]


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    available_product_names = [p.name for p, _ in _model.get_products_with_summaries()]

    for product_name in _model.DEFAULT_START_PAGE_PRODUCTS:
        if product_name in available_product_names:
            default_product = product_name
            break
    else:
        default_product = available_product_names[0]

    return flask.redirect(flask.url_for("overview_page", product_name=default_product))


@app.route("/products.txt")
def product_list_text():
    # This is useful for bash scripts when we want to loop products :)
    return Response(
        "\n".join(_model.STORE.list_complete_products()), content_type="text/plain"
    )
