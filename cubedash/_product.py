from __future__ import absolute_import

import logging

from flask import Blueprint, abort

from cubedash import _model
from cubedash import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__)


@bp.route("/product/<name>")
def product_page(name):
    product = _model.STORE.index.products.get_by_name(name)
    if not product:
        abort(404, f"Unknown product {name!r}")
    ordered_metadata = utils.get_ordered_metadata(product.definition)
    product_summary = _model.get_product_summary(name)

    return utils.render(
        "product.html",
        product=product,
        product_summary=product_summary,
        metadata_doc=ordered_metadata,
    )


@bp.route("/product/<name>.odc-product.yaml")
def raw_product_doc(name):
    product = _model.STORE.index.products.get_by_name(name)
    if not product:
        abort(404, f"Unknown product {name!r}")

    ordered_metadata = utils.get_ordered_metadata(product.definition)
    return utils.as_yaml(ordered_metadata)


@bp.route("/metadata-type/<name>")
def metadata_type_page(name):
    metadata_type = _model.STORE.index.metadata_types.get_by_name(name)
    if not metadata_type:
        abort(404, f"Unknown metadata type {name!r}")
    ordered_metadata = utils.get_ordered_metadata(metadata_type.definition)

    products_using_it = sorted(
        (
            p
            for p in _model.STORE.index.products.get_all()
            if p.metadata_type.name == name
        ),
        key=lambda p: p.name,
    )
    return utils.render(
        "metadata-type.html",
        metadata_type=metadata_type,
        metadata_doc=ordered_metadata,
        products_using_it=products_using_it,
    )


@bp.route("/metadata-type/<name>.odc-type.yaml")
def raw_metadata_type_doc(name):
    metadata_type = _model.STORE.index.metadata_types.get_by_name(name)
    if not metadata_type:
        abort(404, f"Unknown metadata type {name!r}")
    ordered_metadata = utils.get_ordered_metadata(metadata_type.definition)
    return utils.as_yaml(ordered_metadata)
