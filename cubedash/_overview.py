from __future__ import absolute_import

import flask
import structlog
from flask import Blueprint, abort, redirect, url_for
from flask import request
from werkzeug.datastructures import MultiDict

from cubedash import _utils as utils
from cubedash._model import index, as_json, get_summary
from datacube.scripts.dataset import build_dataset_info

_LOG = structlog.getLogger()
bp = Blueprint('product', __name__)

_HARD_SEARCH_LIMIT = 500


# @bp.route('/')
@bp.route('/<product_name>')
@bp.route('/<product_name>/<int:year>')
@bp.route('/<product_name>/<int:year>/<int:month>')
@bp.route('/<product_name>/<int:year>/<int:month>/<int:day>')
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

        product=product,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
    )


# @bp.route('/datasets')
@bp.route('/datasets/<product_name>')
@bp.route('/datasets/<product_name>/<int:year>')
@bp.route('/datasets/<product_name>/<int:year>/<int:month>')
@bp.route('/datasets/<product_name>/<int:year>/<int:month>/<int:day>')
def search_page(product_name: str = None,
                year: int = None,
                month: int = None,
                day: int = None):
    product, product_summary, selected_summary = _load_product(product_name, year, month, day)
    time = utils.as_time_range(year, month, day)

    args = MultiDict(flask.request.args)
    query = utils.query_to_search(args, product=product)
    # Add time range, selected product to query

    if product_name:
        query['product'] = product_name
    if time:
        query['time'] = time

    _LOG.info('query', query=query)

    # TODO: Add sort option to index API
    datasets = sorted(index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
                      key=lambda d: d.center_time)

    if request_wants_json():
        return as_json(dict(
            datasets=[build_dataset_info(index, d) for d in datasets],
        ))
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


@bp.route('/<product_name>/spatial')
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for('product.overview_page', product_name=product_name))


@bp.route('/<product_name>/timeline')
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for('product.overview_page', product_name=product_name))


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
    best = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    return best == 'application/json' and \
           request.accept_mimetypes[best] > \
           request.accept_mimetypes['text/html']
