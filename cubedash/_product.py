from __future__ import absolute_import

import logging

import flask
from flask import Blueprint, abort

from cubedash import _utils as utils
from cubedash._model import STORE

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__, url_prefix="/product")


@bp.route("/<name>")
def product_page(name):
    product = STORE.index.products.get_by_name(name)
    if not product:
        abort(404, "Unknown product %r" % name)
    ordered_metadata = utils.get_ordered_metadata(product.definition)

    return flask.render_template(
        "product.html", product=product, product_metadata=ordered_metadata
    )
