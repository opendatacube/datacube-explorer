from __future__ import absolute_import

import functools
import logging
from datetime import datetime

import flask
from dateutil import tz
from flask import Blueprint, abort, request
from werkzeug.datastructures import MultiDict

from cubedash import _utils as utils
from cubedash._model import as_json, cache, index
from datacube.model import DatasetType, Range
from datacube.scripts.dataset import build_dataset_info

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__, url_prefix="/<product_name>")

_HARD_SEARCH_LIMIT = 500


def with_loaded_product(f):
    """Convert the 'product_name' query argument into a 'product' entity"""

    @functools.wraps(f)
    def wrapper(product_name: str, *args, **kwargs):
        product = index.products.get_by_name(product_name)
        if product is None:
            abort(404, "Unknown product %r" % product_name)
        return f(product, *args, **kwargs)

    return wrapper


@bp.route("/spatial")
@with_loaded_product
def spatial_page(product: DatasetType):
    types = index.datasets.types.get_all()
    return flask.render_template(
        "spatial.html", products=[p.definition for p in types], selected_product=product
    )


@bp.route("/timeline")
@with_loaded_product
def timeline_page(product: DatasetType):
    return flask.render_template(
        "timeline.html",
        timeline=timeline_years(1986, product),
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product,
    )


@bp.route("/search")
@with_loaded_product
def search_page(product: DatasetType):
    args = MultiDict(flask.request.args)

    query = utils.query_to_search(args, product=product)
    _LOG.info("Query %r", query)

    # TODO: Add sort option to index API
    datasets = sorted(
        index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
        key=lambda d: d.center_time,
    )

    if request_wants_json():
        return as_json(dict(datasets=[build_dataset_info(index, d) for d in datasets]))
    return flask.render_template(
        "search.html",
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product,
        datasets=datasets,
        query_params=query,
        result_limit=_HARD_SEARCH_LIMIT,
    )


def request_wants_json():
    best = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return (
        best == "application/json"
        and request.accept_mimetypes[best] > request.accept_mimetypes["text/html"]
    )


@cache.memoize()
def timeline_years(from_year, product):
    # type: (int, DatasetType) -> List

    timeline = index.datasets.count_product_through_time(
        "1 month",
        product=product.name,
        time=Range(datetime(from_year, 1, 1, tzinfo=tz.tzutc()), datetime.utcnow()),
    )
    return list(timeline)
