import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import flask
from datacube.model import Range
from flask import Blueprint, Response, redirect, url_for

from . import _model, _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint(
    "audit",
    __name__,
)


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


@bp.route("/product-audit/")
def legacy_product_audit_page():
    return redirect(url_for(".product_metadata_page"))


@bp.route("/audit/product-metadata")
def product_metadata_page():
    store = _model.STORE
    all_products = {p.name for p in store.index.products.get_all()}
    summarised_products = set(store.list_complete_products())
    unsummarised_product_names = all_products - summarised_products

    extra = {}
    if "timings" in flask.request.args:
        extra["product_timings_iter"] = cached_product_timings()

    return utils.render(
        "audit-metadata-issues.html",
        products_all=all_products,
        products_summarised=summarised_products,
        products_missing=unsummarised_product_names,
        spatial_quality_stats=list(store.get_quality_stats()),
        **extra,
    )


@bp.route("/audit/dataset-counts")
def dscount_report_page():
    return utils.render(
        "dscount-report.html",
        products_period_dscount=_model.get_time_summary_all_products(),
    )


@bp.route("/audit/dataset-counts.csv")
def dsreport_csv():
    return utils.as_csv(
        filename_prefix="datasets-period-report",
        headers=("product_name", "year", "month", "dataset_count"),
        rows=[
            (*period, count)
            for period, count in _model.get_time_summary_all_products().items()
        ],
    )


@bp.route("/product-audit/day-times.txt")
def get_legacy_timings():
    return redirect(url_for(".get_timings_text"))


@bp.route("/audit/day-query-times.txt")
def get_timings_text():
    def respond():
        yield "product\tcount\ttime_ms\n"
        for f in product_timings():
            time_ms = int(f.time_seconds * 1000) if f.time_seconds else ""
            yield f'"{f.name}"\t{f.dataset_count}\t{time_ms}\n'

    return Response(respond(), mimetype="text/plain")
