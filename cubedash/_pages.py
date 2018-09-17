import itertools
from datetime import datetime, timedelta
from typing import Tuple

import flask
import structlog
from flask import abort, redirect, url_for
from flask import request
from werkzeug.datastructures import MultiDict

import cubedash
import datacube
from cubedash import _monitoring
from cubedash.summary import RegionInfo, TimePeriodOverview
from cubedash.summary._stores import ProductSummary
from datacube.model import DatasetType, Range
from datacube.scripts.dataset import build_dataset_info
from . import _filters, _dataset, _product, _platform, _api, _model, _reports
from . import _utils as utils
from ._utils import as_json

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_product.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_reports.bp)

_LOG = structlog.getLogger()

_HARD_SEARCH_LIMIT = app.config.get('CUBEDASH_HARD_SEARCH_LIMIT', 150)

# Add server timings to http headers.
if app.debug or app.config.get('CUBEDASH_SHOW_PERF_TIMES', False):
    _monitoring.init_app_monitoring()


# @app.route('/')
@app.route('/<product_name>')
@app.route('/<product_name>/<int:year>')
@app.route('/<product_name>/<int:year>/<int:month>')
@app.route('/<product_name>/<int:year>/<int:month>/<int:day>')
def overview_page(product_name: str = None,
                  year: int = None,
                  month: int = None,
                  day: int = None):
    product, product_summary, selected_summary = _load_product(product_name, year, month, day)

    return flask.render_template(
        'overview.html',
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
@app.route('/datasets/<product_name>')
@app.route('/datasets/<product_name>/<int:year>')
@app.route('/datasets/<product_name>/<int:year>/<int:month>')
@app.route('/datasets/<product_name>/<int:year>/<int:month>/<int:day>')
def search_page(product_name: str = None,
                year: int = None,
                month: int = None,
                day: int = None):
    product, product_summary, selected_summary = _load_product(product_name, year, month, day)
    time_range = utils.as_time_range(
        year,
        month,
        day,
        tzinfo=_model.STORE.grouping_timezone
    )

    args = MultiDict(flask.request.args)
    query = utils.query_to_search(args, product=product)

    # Always add time range, selected product to query
    if product_name:
        query['product'] = product_name

    if 'time' in query:
        # If they left one end of the range open, fill it in with the product bounds.
        search_time = query['time']
        assert isinstance(search_time, Range)
        if product_summary:
            query['time'] = Range(
                search_time.begin or product_summary.time_earliest,
                search_time.end or product_summary.time_latest + timedelta(days=1)
            )
    # The URL time range always trumps args.
    if time_range:
        query['time'] = time_range

    _LOG.info('query', query=query)

    # TODO: Add sort option to index API
    datasets = sorted(_model.STORE.index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
                      key=lambda d: d.center_time)

    if request_wants_json():
        return as_json(dict(
            datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets],
        ))

    # For display on the page (and future searches).
    if 'time' not in query and product_summary:
        query['time'] = Range(
            product_summary.time_earliest,
            product_summary.time_latest + timedelta(days=1)
        )

    return flask.render_template(
        'search.html',
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
        result_limit=_HARD_SEARCH_LIMIT
    )


@app.route('/region/<product_name>/<region_code>')
@app.route('/region/<product_name>/<region_code>/<int:year>')
@app.route('/region/<product_name>/<region_code>/<int:year>/<int:month>')
@app.route('/region/<product_name>/<region_code>/<int:year>/<int:month>/<int:day>')
def region_page(product_name: str = None,
                region_code: str = None,
                year: int = None,
                month: int = None,
                day: int = None):
    product, product_summary, selected_summary = _load_product(product_name, year, month, day)

    region_info = RegionInfo.for_product(product)
    if not region_info:
        abort(404, f"Product {product_name} has no region specification.")

    datasets = list(_model.STORE.find_datasets_for_region(
        product_name, region_code, year, month, day,
        limit=_HARD_SEARCH_LIMIT
    ))

    if len(datasets) == 1 and 'feelinglucky' in flask.request.args:
        return flask.redirect(url_for('dataset.dataset_page', id_=datasets[0].id))

    if request_wants_json():
        return as_json(dict(
            datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets],
        ))

    return flask.render_template(
        'region.html',
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
        result_limit=_HARD_SEARCH_LIMIT
    )


@app.route('/<product_name>/spatial')
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for('overview_page', product_name=product_name))


@app.route('/<product_name>/timeline')
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for('overview_page', product_name=product_name))


def _load_product(product_name, year, month, day) -> Tuple[DatasetType, ProductSummary, TimePeriodOverview]:
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
    best = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    return best == 'application/json' and \
           request.accept_mimetypes[best] > \
           request.accept_mimetypes['text/html']


@app.route('/about')
def about_page():
    return flask.render_template(
        'about.html'
    )


@app.context_processor
def inject_globals():
    product_summaries = _model.get_products_with_summaries()

    # Group by product type
    def key(t):
        return t[0].fields.get('product_type')

    grouped_product_summarise = sorted(
        (
            (name or '', list(items))
            for (name, items) in
            itertools.groupby(sorted(product_summaries, key=key), key=key)
        ),
        # Show largest groups first
        key=lambda k: len(k[1]), reverse=True
    )

    return dict(
        grouped_products=grouped_product_summarise,
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
        app_version=cubedash.__version__,
        last_updated_time=_model.get_last_updated(),
    )


@app.route('/')
def default_redirect():
    """Redirect to default starting page."""
    available_product_names = [p.name for p, _ in _model.get_products_with_summaries()]

    for product_name in _model.DEFAULT_START_PAGE_PRODUCTS:
        if product_name in available_product_names:
            default_product = product_name
            break
    else:
        default_product = available_product_names[0]

    return flask.redirect(
        flask.url_for(
            'overview_page',
            product_name=default_product
        )
    )
