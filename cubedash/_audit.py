from __future__ import absolute_import

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import flask
from flask import Blueprint

from datacube.model import Range

from . import _model

_LOG = logging.getLogger(__name__)
bp = Blueprint("audit", __name__, url_prefix="/product-audit")


@dataclass
class ProductTiming:
    name: str
    dataset_count: int
    time_seconds: float = None
    selection_date: datetime = None


def product_timings() -> Iterable[ProductTiming]:
    """
    How long does it take to query a day?
    Useful for finding missing time indexes..
    """
    done = 0
    store = _model.STORE
    for product_name in store.list_complete_products():

        p = store.get_product_summary(product_name)

        if not p:
            _LOG.info("product_no_summarised", product_name=product_name)
            continue
        if not p.dataset_count or not p.time_earliest:
            yield ProductTiming(product_name, dataset_count=0)
            continue
        done += 1
        middle_period = p.time_earliest + (p.time_latest - p.time_earliest) / 2
        day = middle_period.replace(hour=0, minute=0, second=0)

        start = time.time()
        dataset_count = store.index.datasets.count(
            product=product_name, time=Range(day, day + timedelta(days=1))
        )
        end = time.time()
        yield ProductTiming(product_name, dataset_count, end - start, day)


@_model.cache.memoize()
def cached_product_timings():
    return sorted(
        list(product_timings()), key=lambda a: a.time_seconds or 0, reverse=True
    )


@bp.route("/")
def product_audit_page():
    store = _model.STORE
    all_products = set(p.name for p in store.index.products.get_all())
    summarised_products = set(store.list_complete_products())
    unsummarised_product_names = all_products - summarised_products

    extra = {}
    if "timings" in flask.request.args:
        extra["product_timings_iter"] = cached_product_timings()

    return flask.render_template(
        "product-audit.html",
        products_all=all_products,
        products_summarised=summarised_products,
        products_missing=unsummarised_product_names,
        spatial_quality_stats=list(store.get_quality_stats()),
        **extra,
    )
