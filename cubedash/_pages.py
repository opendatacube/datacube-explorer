import itertools
import re
from datetime import datetime, timedelta
from typing import List, Tuple

import datacube
import flask
import structlog
from datacube.model import DatasetType, Range
from datacube.scripts.dataset import build_dataset_info
from flask import abort, redirect, request, url_for
from werkzeug.datastructures import MultiDict

import cubedash
from cubedash import _audit, _monitoring
from cubedash._model import ProductWithSummary
from cubedash.summary import TimePeriodOverview
from cubedash.summary._stores import ProductSummary

from . import (
    _api,
    _dataset,
    _filters,
    _model,
    _platform,
    _product,
    _stac,
    _stac_legacy,
    _utils as utils,
)
from ._utils import as_rich_json

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_product.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_audit.bp)
app.register_blueprint(_stac.bp)
app.register_blueprint(_stac_legacy.bp)

_LOG = structlog.getLogger()

_HARD_SEARCH_LIMIT = app.config.get("CUBEDASH_HARD_SEARCH_LIMIT", 150)
_DEFAULT_GROUP_NAME = app.config.get("CUBEDASH_DEFAULT_GROUP_NAME", "Other Products")

_DEFAULT_ARRIVALS_DAYS: int = app.config.get("CUBEDASH_DEFAULT_ARRIVALS_DAY_COUNT", 14)

# Add server timings to http headers.
if app.config.get("CUBEDASH_SHOW_PERF_TIMES", False):
    _monitoring.init_app_monitoring()


@app.route("/<product_name>")
@app.route("/<product_name>/<int:year>")
@app.route("/<product_name>/<int:year>/<int:month>")
@app.route("/<product_name>/<int:year>/<int:month>/<int:day>")
@app.route("/product/<product_name>")
@app.route("/products/<product_name>/extents")
@app.route("/products/<product_name>/extents/<int:year>")
@app.route("/products/<product_name>/extents/<int:year>/<int:month>")
@app.route("/products/<product_name>/extents/<int:year>/<int:month>/<int:day>")
def legacy_product_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    return redirect(
        url_for(
            ".product_page", product_name=product_name, year=year, month=month, day=day
        )
    )


@app.route("/products/<product_name>")
@app.route("/products/<product_name>/<int:year>")
@app.route("/products/<product_name>/<int:year>/<int:month>")
@app.route("/products/<product_name>/<int:year>/<int:month>/<int:day>")
def product_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    (
        product,
        product_summary,
        selected_summary,
        year_selector_summary,
        time_selector_summary,
    ) = _load_product(product_name, year, month, day)

    default_zoom = flask.current_app.config["default_map_zoom"]
    default_center = flask.current_app.config["default_map_center"]

    region_geojson = _model.get_regions_geojson(product_name, year, month, day)

    return utils.render(
        "product.html",
        year=year,
        month=month,
        day=day,
        # Which data to preload with the page?
        regions_geojson=region_geojson,
        datasets_geojson=None,  # _model.get_datasets_geojson(product_name, year, month, day),
        footprint_geojson=_model.get_footprint_geojson(product_name, year, month, day),
        product=product,
        product_region_info=_model.STORE.get_product_region_info(product_name)
        if region_geojson
        else None,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
        # Map defaults
        default_zoom=default_zoom,
        default_center=default_center,
        year_selector_summary=year_selector_summary,
        time_selector_summary=time_selector_summary,
        location_samples=_model.STORE.product_location_samples(
            product.name, year, month, day
        ),
        metadata_doc=(utils.prepare_document_formatting(product.definition)),
    )


@app.route("/datasets/<product_name>")
@app.route("/datasets/<product_name>/<int:year>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def legacy_search_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    return redirect(
        url_for(
            ".search_page",
            product_name=product_name,
            year=year,
            month=month,
            day=day,
            **request.args,
        )
    )


@app.route("/products/<product_name>/datasets")
@app.route("/products/<product_name>/datasets/<int:year>")
@app.route("/products/<product_name>/datasets/<int:year>/<int:month>")
@app.route("/products/<product_name>/datasets/<int:year>/<int:month>/<int:day>")
def search_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    (
        product,
        product_summary,
        selected_summary,
        year_selector_summary,
        time_selector_summary,
    ) = _load_product(product_name, year, month, day)
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
        _model.STORE.index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT + 1),
        key=lambda d: d.center_time,
    )
    more_datasets_exist = False
    if len(datasets) > _HARD_SEARCH_LIMIT:
        more_datasets_exist = True
        datasets = datasets[:_HARD_SEARCH_LIMIT]

    if request_wants_json():
        return as_rich_json(
            dict(datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets])
        )

    # For display on the page (and future searches).
    if "time" not in query and product_summary and product_summary.time_earliest:
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
        there_are_more_results=more_datasets_exist,
        time_selector_summary=time_selector_summary,
        year_selector_summary=year_selector_summary,
    )


@app.route("/region/<product_name>/<region_code>")
@app.route("/region/<product_name>/<region_code>/<int:year>")
@app.route("/region/<product_name>/<region_code>/<int:year>/<int:month>")
@app.route("/region/<product_name>/<region_code>/<int:year>/<int:month>/<int:day>")
def legacy_region_page(
    product_name: str = None,
    region_code: str = None,
    year: int = None,
    month: int = None,
    day: int = None,
):
    return redirect(
        url_for(
            ".region_page",
            product_name=product_name,
            region_code=region_code,
            year=year,
            month=month,
            day=day,
            **request.args,
        )
    )


@app.route("/product/<product_name>/regions")
def regions_page(product_name: str):
    # A map of regions is shown on the overview page.
    return redirect(
        url_for(
            ".product_page",
            product_name=product_name,
        )
    )


@app.route("/product/<product_name>/regions/<region_code>")
@app.route("/product/<product_name>/regions/<region_code>/<int:year>")
@app.route("/product/<product_name>/regions/<region_code>/<int:year>/<int:month>")
@app.route(
    "/product/<product_name>/regions/<region_code>/<int:year>/<int:month>/<int:day>"
)
def region_page(
    product_name: str = None,
    region_code: str = None,
    year: int = None,
    month: int = None,
    day: int = None,
):
    (
        product,
        product_summary,
        selected_summary,
        year_selector_summary,
        time_selector_summary,
    ) = _load_product(product_name, year, month, day)

    region_info = _model.STORE.get_product_region_info(product_name)
    if not region_info:
        abort(404, f"Product {product_name!r} has no region specification.")

    if region_info.region(region_code) is None:
        abort(404, f"Product {product_name!r} has no {region_code!r} region.")

    offset = flask.request.args.get("_o", default=0, type=int)
    limit = _HARD_SEARCH_LIMIT
    datasets = list(
        _model.STORE.find_datasets_for_region(
            product_name, region_code, year, month, day, limit=limit + 1, offset=offset
        )
    )

    def url_with_offset(new_offset: int):
        """Currently request url with a different offset."""
        page_args = dict(flask.request.view_args)
        page_args["_o"] = new_offset
        return url_for(".region_page", **page_args)

    next_page_url = None
    if len(datasets) > limit:
        datasets = datasets[:limit]
        next_page_url = url_with_offset(offset + limit)

    previous_page_url = None
    if offset > 0:
        previous_page_url = url_with_offset(max(offset - _HARD_SEARCH_LIMIT, 0))

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
        previous_page_url=previous_page_url,
        next_page_url=next_page_url,
        time_selector_summary=time_selector_summary,
        year_selector_summary=year_selector_summary,
    )


@app.route("/product/<product_name>/regions/<region_code>.geojson")
@app.route("/product/<product_name>/regions/<region_code>/<int:year>.geojson")
@app.route(
    "/product/<product_name>/regions/<region_code>/<int:year>/<int:month>.geojson"
)
@app.route(
    "/product/<product_name>/regions/<region_code>/<int:year>/<int:month>/<int:day>.geojson"
)
def region_geojson(
    product_name: str = None,
    region_code: str = None,
    year: int = None,
    month: int = None,
    day: int = None,
):
    region_info = _model.STORE.get_product_region_info(product_name)
    if not region_info:
        abort(404, f"Product {product_name!r} has no region specification.")

    if region_info.region(region_code) is None:
        abort(404, f"Product {product_name!r} has no {region_code!r} region.")

    geojson = region_info.region(region_code).footprint_geojson
    geojson["properties"].update(
        dict(
            product_name=product_name,
            year_month_day_filter=[year, month, day],
        )
    )
    return utils.as_geojson(
        geojson,
        downloadable_filename_prefix=utils.api_path_as_filename_prefix(),
    )


@app.route("/<product_name>/spatial")
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("product_page", product_name=product_name))


@app.route("/<product_name>/timeline")
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("product_page", product_name=product_name))


def _load_product(
    product_name, year, month, day
) -> Tuple[
    DatasetType,
    ProductSummary,
    TimePeriodOverview,
    TimePeriodOverview,
    TimePeriodOverview,
]:
    product = None
    if product_name:
        try:
            product = _model.STORE.get_dataset_type(product_name)
        except KeyError:
            abort(404, f"Unknown product {product_name!r}")

    product_summary = _model.get_product_summary(product_name)
    time_summary = _model.get_time_summary(product_name, year, month, day)
    year_selector_summary = _model.get_time_summary(product_name, None, None, None)
    time_selector_summary = _model.get_time_summary(product_name, year, None, None)
    return (
        product,
        product_summary,
        time_summary,
        year_selector_summary,
        time_selector_summary,
    )


def request_wants_json():
    best = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return (
        best == "application/json"
        and request.accept_mimetypes[best] > request.accept_mimetypes["text/html"]
    )


@app.context_processor
def inject_globals():
    # The footer "Last updated" date.
    # The default is the currently-viewed product's summary refresh date.
    last_updated = None
    if "product_name" in flask.request.view_args:
        product_summary = _model.STORE.get_product_summary(
            flask.request.view_args["product_name"]
        )
        if product_summary:
            last_updated = product_summary.last_successful_summary_time

    return dict(
        # Only the known, summarised products in groups.
        grouped_products=_get_grouped_products(),
        # All products in the datacube, summarised or not.
        datacube_products=list(_model.STORE.index.products.get_all()),
        datacube_metadata_types=list(_model.STORE.index.metadata_types.get_all()),
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
        app_version=cubedash.__version__,
        grouping_timezone=_model.STORE.grouping_timezone,
        last_updated_time=last_updated,
        explorer_instance_title=app.config.get(
            "CUBEDASH_INSTANCE_TITLE",
        )
        or app.config.get("STAC_ENDPOINT_TITLE", ""),
        breadcrumb=_get_breadcrumbs(request.path, request.script_root),
    )


HREF = str
SHOULD_LINK = bool


def _get_breadcrumbs(url: str, script_root: str) -> List[Tuple[HREF, str, SHOULD_LINK]]:
    """
    >>> _get_breadcrumbs('/products/great_product', '/')
    [('/products', 'products', True), ('/products/great_product', 'great_product', False)]
    >>> _get_breadcrumbs('/products/great_product', '/prefix')
    [('/prefix/products', 'products', True), ('/prefix/products/great_product', 'great_product', False)]
    >>> _get_breadcrumbs('/products', '/')
    [('/products', 'products', False)]
    >>> _get_breadcrumbs('/products', '/pref')
    [('/pref/products', 'products', False)]
    >>> _get_breadcrumbs('/', '/')
    []
    >>> _get_breadcrumbs('/', '/pref')
    []
    >>> _get_breadcrumbs('', '/')
    []
    """
    breadcrumb = []
    i = 2
    script_root = script_root.rstrip("/")

    for part_name in url.split("/"):
        if part_name:
            part_href = "/".join(url.split("/")[:i])
            breadcrumb.append(
                (
                    f"{script_root}{part_href}",
                    part_name,
                    # Don't link to the current page.
                    part_href != url,
                )
            )
            i += 1
    return breadcrumb


def _get_grouped_products() -> List[Tuple[str, List[ProductWithSummary]]]:
    """
    We group products using the configured grouping field (default "product_type").

    Anything left ungrouped will be placed at the end in groups of
    configurable max size.
    """
    product_summaries = _model.get_products()
    # Which field should we use when grouping products in the top menu?
    group_by_field = app.config.get("CUBEDASH_PRODUCT_GROUP_BY_FIELD", "product_type")
    group_field_size = app.config.get("CUBEDASH_PRODUCT_GROUP_SIZE", 5)
    group_by_regex = app.config.get("CUBEDASH_PRODUCT_GROUP_BY_REGEX", None)

    if group_by_regex:
        try:
            regex_group = {}
            for regex, group in group_by_regex:
                regex_group[re.compile(regex)] = group.strip()
        except re.error as e:
            raise RuntimeError(
                f"Invalid regexp in CUBEDASH_PRODUCT_GROUP_BY_REGEX for group {group!r}: {e!r}"
            )

    if group_by_regex:
        # group using regex
        def regex_key(t):
            for regex, group in regex_group.items():
                if regex.search(t[0].name):
                    return group
            return _DEFAULT_GROUP_NAME

        key = regex_key
    else:
        # Group using the configured key, or fall back to the product name.
        def field_key(t):
            return t[0].fields.get(group_by_field) or _DEFAULT_GROUP_NAME

        key = field_key

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
    return _partition_default(grouped_product_summarise, group_field_size)


def _partition_default(
    grouped_product_summarise: List[Tuple[str, List[ProductWithSummary]]],
    remainder_group_size=5,
) -> List[Tuple[str, List[ProductWithSummary]]]:
    """
    For default items and place them at the end in batches.
    """
    lonely_products = []
    for i, group_tuple in enumerate(grouped_product_summarise.copy()):
        if group_tuple[0] == _DEFAULT_GROUP_NAME:
            lonely_products = group_tuple[1]
            grouped_product_summarise.pop(i)
            break

    there_are_groups = len(grouped_product_summarise) > 0

    lonely_products = sorted(lonely_products, key=lambda p: p[0].name)
    for i, lonely_group in enumerate(chunks(lonely_products, remainder_group_size)):
        group_name = ""
        if i == 0:
            group_name = _DEFAULT_GROUP_NAME if there_are_groups else "Products"
        grouped_product_summarise.append((group_name, lonely_group))
    return grouped_product_summarise


def chunks(ls: List, n: int):
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
    for i in range(0, len(ls), n):
        yield ls[i : i + n]


@app.route("/arrivals")
def arrivals_page():

    period_length = timedelta(days=_DEFAULT_ARRIVALS_DAYS)
    arrivals = list(_model.STORE.get_arrivals(period_length=period_length))
    return utils.render(
        "arrivals.html",
        arrival_days=arrivals,
        period_length=period_length,
    )


@app.route("/arrivals.csv")
def arrivals_csv():
    period_length = timedelta(days=_DEFAULT_ARRIVALS_DAYS)

    def _flat_arrivals_rows():
        for _, arrivals in _model.STORE.get_arrivals(period_length=period_length):
            for arrival in arrivals:
                yield (
                    arrival.day,
                    arrival.product_name,
                    arrival.dataset_count,
                    [str(dataset_id) for dataset_id in arrival.sample_dataset_ids],
                )

    return utils.as_csv(
        filename_prefix="recent-arrivals",
        headers=("day", "product_name", "dataset_count", "sample_dataset_ids"),
        rows=_flat_arrivals_rows(),
    )


@app.route("/about")
def about_page():
    return utils.render(
        "about.html",
        total_dataset_count=(
            sum(
                summary.dataset_count
                for product, summary in _model.get_products_with_summaries()
            )
        ),
        stac_version=_stac.STAC_VERSION,
        stac_endpoint_config=_stac.stac_endpoint_information(),
        explorer_root_url=url_for("default_redirect", _external=True),
    )


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(flask.url_for("product.products_page"))
