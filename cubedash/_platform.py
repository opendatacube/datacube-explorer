from __future__ import absolute_import

import logging
from datetime import datetime

import flask
from cachetools.func import ttl_cache
from dateutil import tz
from flask import Blueprint
from werkzeug.datastructures import Range

from cubedash._model import CACHE_LONG_TIMEOUT_SECS, index

_LOG = logging.getLogger(__name__)
bp = Blueprint("platform", __name__)

_HARD_SEARCH_LIMIT = 500


@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def _timelines_platform(platform):
    products = index.datasets.count_by_product_through_time(
        "1 month",
        platform=platform,
        time=Range(datetime(1986, 1, 1, tzinfo=tz.tzutc()), datetime.utcnow()),
    )
    return list(products)


@bp.route("/platform/<platform>")
def platform_page(platform):
    return flask.render_template(
        "platform.html",
        product_counts=_timelines_platform(platform),
        products=[p.definition for p in index.datasets.types.get_all()],
        platform=platform,
    )
