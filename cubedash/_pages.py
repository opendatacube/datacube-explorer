import itertools
from datetime import datetime

import flask
import structlog
from flask import abort, redirect, request, url_for
from werkzeug.datastructures import MultiDict

import cubedash
import datacube
from cubedash import _utils as utils
from cubedash._model import as_json, get_summary, index
from datacube.scripts.dataset import build_dataset_info

from . import _api, _dataset, _filters, _model, _platform, _product

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_platform.bp)

_LOG = structlog.getLogger()

_HARD_SEARCH_LIMIT = 500


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

    return flask.render_template(
        "overview.html",
        year=year,
        month=month,
        day=day,
        product=product,
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
    time = utils.as_time_range(year, month, day)

    args = MultiDict(flask.request.args)
    query = utils.query_to_search(args, product=product)
    # Add time range, selected product to query

    if product_name:
        query["product"] = product_name
    if time:
        query["time"] = time

    _LOG.info("query", query=query)

    # TODO: Add sort option to index API
    datasets = sorted(
        index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
        key=lambda d: d.center_time,
    )

    if request_wants_json():
        return as_json(dict(datasets=[build_dataset_info(index, d) for d in datasets]))
    return flask.render_template(
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


@app.route("/<product_name>/spatial")
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


@app.route("/<product_name>/timeline")
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


def _load_product(product_name, year, month, day):
    product = None
    if product_name:
        product = index.products.get_by_name(product_name)
        if not product:
            abort(404, "Unknown product %r" % product_name)

    # Entire summary for the product.
    product_summary = get_summary(product_name)
    selected_summary = get_summary(product_name, year, month, day)

    return product, product_summary, selected_summary


def request_wants_json():
    best = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return (
        best == "application/json"
        and request.accept_mimetypes[best] > request.accept_mimetypes["text/html"]
    )


@app.route("/about")
def about_page():
    return flask.render_template("about.html")


@app.context_processor
def inject_globals():
    product_summaries = _model.list_product_summaries()

    # Group by product type
    def key(t):
        return t[0].fields.get("product_type")

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

    return dict(
        products=product_summaries,
        grouped_products=grouped_product_summarise,
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
        app_version=cubedash.__version__,
    )


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(flask.url_for("overview_page", product_name="ls7_nbar_scene"))
