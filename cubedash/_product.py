from __future__ import absolute_import

import flask
from cachetools.func import ttl_cache

from datetime import datetime

from datacube.scripts.dataset import build_dataset_info
from dateutil import tz
from datacube.model import Range
from flask import request
from werkzeug.datastructures import MultiDict

from cubedash import _utils as utils
import logging
from cubedash._model import CACHE_LONG_TIMEOUT_SECS, index, as_json

from flask import Blueprint

_LOG = logging.getLogger(__name__)
bp = Blueprint('product', __name__)

_HARD_SEARCH_LIMIT = 500


@bp.route('/<product>/spatial')
def spatial_page(product):
    types = index.datasets.types.get_all()
    return flask.render_template(
        'spatial.html',
        products=[p.definition for p in types],
        selected_product=product
    )


@bp.route('/<product>/timeline')
def timeline_page(product):
    return flask.render_template(
        'timeline.html',
        timeline=_timeline_years(1986, product),
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product
    )


@bp.route('/<product>/datasets')
def datasets_page(product: str):
    product_entity = index.products.get_by_name_unsafe(product)
    args = MultiDict(flask.request.args)

    query = utils.query_to_search(args, product=product_entity)
    _LOG.info('Query %r', query)

    # TODO: Add sort option to index API
    datasets = sorted(index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT), key=lambda d: d.center_time)

    if request_wants_json():
        return as_json(dict(
            datasets=[build_dataset_info(index, d) for d in datasets],
        ))
    return flask.render_template(
        'datasets.html',
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product,
        selected_product_e=product_entity,
        datasets=datasets,
        query_params=query
    )


def request_wants_json():
    best = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    return best == 'application/json' and \
           request.accept_mimetypes[best] > \
           request.accept_mimetypes['text/html']


@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def _timeline_years(from_year, product):
    timeline = index.datasets.count_product_through_time(
        '1 month',
        product=product,
        time=Range(
            datetime(from_year, 1, 1, tzinfo=tz.tzutc()),
            datetime.utcnow()
        )
    )
    return list(timeline)
