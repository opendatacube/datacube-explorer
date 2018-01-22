from __future__ import absolute_import

import functools
import logging
from datetime import datetime
from typing import List

import flask
from dateutil import tz
from flask import Blueprint, abort, redirect, request, url_for
from werkzeug.datastructures import MultiDict

from cubedash import _utils as utils
from cubedash._model import as_json, cache, get_summary, index
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


@bp.route("/")
@with_loaded_product
def overview_page(product: DatasetType):
    year, month, day = y_m_d()
    summary = get_summary(product.name, year, month, day)

    return flask.render_template(
        "product.html",
        summary=summary,
        year=year,
        month=month,
        day=day,
        selected_product=product,
    )


def y_m_d():
    year = request.args.get("year", None, type=int)
    month = request.args.get("month", None, type=int)
    day = request.args.get("day", None, type=int)
    return year, month, day


def time_range_args() -> Range:
    return utils.as_time_range(*y_m_d())


@bp.route("/spatial")
@with_loaded_product
def spatial_page(product: DatasetType):
    return redirect(url_for("product.overview_page", product_name=product.name))


@bp.route("/timeline")
@with_loaded_product
def timeline_page(product: DatasetType):
    return redirect(url_for("product.overview_page", product_name=product.name))


@bp.route("/search")
@with_loaded_product
def search_page(product: DatasetType):
    time = time_range_args()

    args = MultiDict(flask.request.args)
    # Already retrieved
    args.pop("year", None)
    args.pop("month", None)
    args.pop("day", None)

    query = utils.query_to_search(args, product=product)
    # Add time range, selected product to query

    query["product"] = product.name

    if time:
        query["time"] = time

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
def timeline_years(from_year: int, product: DatasetType) -> List:
    timeline = index.datasets.count_product_through_time(
        "1 month",
        product=product.name,
        time=Range(datetime(from_year, 1, 1, tzinfo=tz.tzutc()), datetime.utcnow()),
    )
    return list(timeline)
