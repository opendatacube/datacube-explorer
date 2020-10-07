from __future__ import absolute_import

import csv
import io
import logging
import re

import flask
from flask import Blueprint, abort, url_for, redirect, Response

from cubedash import _model, _utils
from cubedash import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__)


def _product_sample_information():
    product_summary_uris = []
    for product, summary in _model.get_products_with_summaries():
        product_summary_uris.append(
            (
                product,
                summary,
                _model.STORE.product_location_prefixes(product.name),
            )
        )
    return product_summary_uris


@bp.route("/about.csv")
def products_csv():
    """Get the products table as a CSV"""
    out = io.StringIO()
    cw = csv.writer(out)
    cw.writerow(
        ["name", "count", "locations", "license", "definition", "metadata_type"]
    )
    cw.writerows(
        (
            product.name,
            summary.dataset_count,
            uri_samples,
            _utils.product_license(product),
            url_for("product.raw_product_doc", name=product.name, _external=True),
            product.metadata_type.name,
        )
        for product, summary, uri_samples in _product_sample_information()
    )
    this_explorer_id = _only_alphanumeric(
        _model.app.config.get("STAC_ENDPOINT_ID", "explorer")
    )

    response = flask.make_response(out.getvalue())
    response.headers[
        "Content-Disposition"
    ] = f"attachment; filename=product-information-{this_explorer_id}.csv"
    response.headers["Content-type"] = "text/csv"
    return response


@bp.route("/products.txt")
def product_list_text():
    # This is useful for bash scripts when we want to loop products :)
    return Response(
        "\n".join(_model.STORE.list_complete_products()), content_type="text/plain"
    )


def _only_alphanumeric(s: str):
    return re.sub("[^0-9a-zA-Z]+", "-", s)


@bp.route("/about")
def products_page():
    return utils.render(
        "about.html", product_summary_uris=_product_sample_information()
    )


@bp.route("/product")
def product_redirect():
    """
    If people remove the name from a "/product/<name>" url, take them somewhere useful
    """
    return redirect(url_for(".products_page"))


@bp.route("/product/<name>")
def product_page(name):
    product = _model.STORE.index.products.get_by_name(name)
    if not product:
        abort(404, f"Unknown product {name!r}")
    ordered_metadata = utils.prepare_document_formatting(product.definition)
    product_summary = _model.get_product_summary(name)

    return utils.render(
        "product.html",
        product=product,
        product_summary=product_summary,
        location_prefixes=_model.STORE.product_location_prefixes(name),
        metadata_doc=ordered_metadata,
    )


@bp.route("/product/<name>.odc-product.yaml")
def raw_product_doc(name):
    product = _model.STORE.index.products.get_by_name(name)
    if not product:
        abort(404, f"Unknown product {name!r}")

    ordered_metadata = utils.prepare_document_formatting(
        product.definition, "Product", include_source_url=True
    )
    return utils.as_yaml(ordered_metadata)


@bp.route("/metadata-type/<name>")
def metadata_type_page(name):
    metadata_type = _model.STORE.index.metadata_types.get_by_name(name)
    if not metadata_type:
        abort(404, f"Unknown metadata type {name!r}")
    ordered_metadata = utils.prepare_document_formatting(metadata_type.definition)

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
    ordered_metadata = utils.prepare_document_formatting(
        metadata_type.definition, "Metadata Type", include_source_url=True
    )
    return utils.as_yaml(ordered_metadata)
