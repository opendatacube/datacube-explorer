import logging
import flask
from flask import Blueprint, abort
import re
from . import _model

_LOG = logging.getLogger(__name__)
bp = Blueprint('reports', __name__, url_prefix='/reports')


# @app.route('/reports')
@bp.route('/')
def reports_page():
    return flask.render_template(
        'reports.html'
    )


# @app.route('/reports')
@bp.route('/<product_name_list>')
@bp.route('/<product_name_list>/<int:year>')
@bp.route('/<product_name_list>/<int:year>/<int:month>')
@bp.route('/<product_name_list>/<int:year>/<int:month>/<int:day>')
def report_products_page(product_name_list: str = None,
                         year: int = None,
                         month: int = None,
                         day: int = None):
    product_names = re.split('\+', product_name_list)
    products = []
    for product_name in product_names:
        product, product_summary, selected_summary = _load_product(product_name, year, month, day)
        products.append({'product': product, 'product_summary': product_summary, 'selected_summary': selected_summary})

    return flask.render_template(
        'product_summary.html',
        year=year,
        month=month,
        day=day,
        products=products,
    )


# @app.route(/reports/time
@bp.route('/time')
@bp.route('/time/<int:year>')
@bp.route('/time/<int:year>/<int:month>')
def reports_time_page(year: int = None,
                      month: int = None):
    return flask.render_template(
        'reports-time.html',
        year=year,
        month=month
    )


def _load_product(product_name, year, month, day):
    product = None
    if product_name:
        product = _model.index.products.get_by_name(product_name)
        if not product:
            abort(404, "Unknown product %r" % product_name)

    # Entire summary for the product.
    product_summary = _model.get_summary(product_name)
    selected_summary = _model.get_summary(product_name, year, month, day)

    return product, product_summary, selected_summary
