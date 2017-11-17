from __future__ import absolute_import

import logging

import flask
from flask import Blueprint

from cubedash import _product
from cubedash._model import index

_LOG = logging.getLogger(__name__)
bp = Blueprint('platform', __name__, url_prefix='/platform')

_HARD_SEARCH_LIMIT = 500


def _timelines_platform(platform_name):
    for product in index.products.search(platform=platform_name):
        _LOG.debug("Building timeline for platform product %s", product.name)
        yield product, _product._timeline_years(1986, product)


@bp.route('/<platform_name>')
def platforms_page(platform_name):
    return flask.render_template(
        'platform.html',
        product_counts=_timelines_platform(platform_name),
        products=[p.definition for p in index.datasets.types.get_all()],
        platform=platform_name
    )
