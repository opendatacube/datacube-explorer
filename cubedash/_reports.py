import logging
import re

import flask
from flask import Blueprint, abort

from cubedash import _utils as utils

from . import _model

_LOG = logging.getLogger(__name__)
bp = Blueprint("reports", __name__, url_prefix="/reports")


# @app.route('/reports')
@bp.route("/")
def reports_page():
    return utils.render("reports.html")


# @app.route('/reports')
@bp.route("/<product_name_list>")
@bp.route("/<product_name_list>/<int:year>")
@bp.route("/<product_name_list>/<int:year>/<int:month>")
@bp.route("/<product_name_list>/<int:year>/<int:month>/<int:day>")
def report_products_page(
    product_name_list: str = None, year: int = None, month: int = None, day: int = None
):
    product_names = re.split(r"\+", product_name_list)
    products = []
    for product_name in product_names:
        product, product_summary, selected_summary = _load_product(
            product_name, year, month, day
        )
        products.append(
            {
                "product": product,
                "product_summary": product_summary,
                "selected_summary": selected_summary,
            }
        )

    return utils.render(
        "product_summary.html", year=year, month=month, day=day, products=products
    )


# @app.route('/reports/time/report_type')
@bp.route("time/<report_type>")
@bp.route("time/<report_type>/<int:year>")
@bp.route("time/<report_type>/<int:year>/<int:month>")
@bp.route("time/<report_type>/<int:year>/<int:month>/<int:day>")
def reports_time_page(
    report_type="", year: int = None, month: int = None, day: int = None
):
    return utils.render(
        "reports-time.html", report_type=report_type, year=year, month=month, day=day
    )


# @app.route('/reports/differences')
@bp.route("/differences/<product_name_list>")
@bp.route("/differences/<product_name_list>/<int:year>")
@bp.route("/differences/<product_name_list>/<int:year>/<int:month>")
@bp.route("/differences/<product_name_list>/<int:year>/<int:month>/<int:day>")
def differences(
    product_name_list: str = None, year: int = None, month: int = None, day: int = None
):
    product_names = re.split(r"\+", product_name_list)
    product_1, product_summary_1, selected_summary_1 = _load_product(
        product_names[0], year, month, day
    )
    product_2, product_summary_2, selected_summary_2 = _load_product(
        product_names[1], year, month, day
    )
    # import ipdb; ipdb.set_trace()
    product = lambda: None
    product.name = product_names[0] + "-" + product_names[1]
    if selected_summary_1 and selected_summary_2:
        diff_counts = (
            selected_summary_1.timeline_dataset_counts
            - selected_summary_2.timeline_dataset_counts
        ) + (
            selected_summary_2.timeline_dataset_counts
            - selected_summary_1.timeline_dataset_counts
        )
    elif selected_summary_1 and not selected_summary_2:
        diff_counts = selected_summary_1.timeline_dataset_counts
    elif selected_summary_2 and not selected_summary_1:
        diff_counts = selected_summary_2.timeline_dataset_counts
    else:
        diff_counts = None
    selected_summary = lambda: None
    selected_summary.timeline_dataset_counts = diff_counts
    selected_summary.dataset_count = sum(diff_counts.values()) if diff_counts else 0
    selected_summary.timeline_period = (
        selected_summary_1.timeline_period if selected_summary_1 else None
    )
    products = []
    product_summary = None
    products.append(
        {
            "product": product,
            "product_summary": product_summary,
            "selected_summary": selected_summary,
        }
    )
    return utils.render(
        "product_summary.html", year=year, month=month, day=day, products=products
    )


def _load_product(product_name, year, month, day):
    product = None
    if product_name:
        product = _model.STORE.index.products.get_by_name(product_name)
        if not product:
            abort(404, "Unknown product %r" % product_name)

    # Entire summary for the product.
    product_summary = _model.get_time_summary(product_name)
    selected_summary = _model.get_time_summary(product_name, year, month, day)

    return product, product_summary, selected_summary
