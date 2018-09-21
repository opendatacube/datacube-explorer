from __future__ import absolute_import

import logging
import time
from datetime import timedelta, datetime
from typing import Iterable

import flask
from dataclasses import dataclass
from flask import Blueprint

from cubedash._model import STORE, cache
from datacube.model import Range

_LOG = logging.getLogger(__name__)
bp = Blueprint('audit', __name__, url_prefix='/product-audit')


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
    for product_name in STORE.list_complete_products():

        p = STORE.get_product_summary( product_name)

        if not p:
            _LOG.info("product_no_summarised", product_name= product_name)
            continue
        if not p.dataset_count or not p.time_earliest:
            yield ProductTiming( product_name, dataset_count=0)
            continue
        done += 1
        middle_period = p.time_earliest + (p.time_latest - p.time_earliest) / 2
        day = middle_period.replace(hour=0, minute=0, second=0)

        start = time.time()
        dataset_count = STORE.index.datasets.count(
            product= product_name,
            time=Range(day, day + timedelta(days=1))
        )
        end = time.time()
        yield ProductTiming(product_name, dataset_count, end - start, day)


@cache.memoize()
def cached_product_timings():
    return sorted(
        list(product_timings()),
        key=lambda a: a.time_seconds or 0,
        reverse=True
    )


@bp.route('/')
def product_audit_page():
    all_products = set(p.name for p in STORE.index.products.get_all())
    summarised_products = set(STORE.list_complete_products())
    unsummarised_product_names = all_products - summarised_products

    extra = {}
    if 'timings' in flask.request.args:
        extra['product_timings_iter'] = cached_product_timings()

    return flask.render_template(
        'product-audit.html',
        products_all=all_products,
        products_summarised=summarised_products,
        products_missing=unsummarised_product_names,
        spatial_quality_stats=list(STORE.get_quality_stats()),
    )
