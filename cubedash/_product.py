from __future__ import absolute_import

import csv
import io
import logging
import re
from datetime import timedelta

import flask
from flask import Blueprint, abort, url_for, redirect, Response

from cubedash import _model, _utils
from cubedash import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__)


@bp.route("/about.csv")
def products_csv():
    """Get the products table as a CSV"""
    out = io.StringIO()
    cw = csv.writer(out)
    cw.writerow(
        [
            "name",
            "count",
            "locations",
            "license",
            "definition",
            "summary_age",
            "metadata_type",
        ]
    )
    cw.writerows(
        (
            product.name,
            summary.dataset_count,
            [
                location.common_prefix
                for location in _model.STORE.product_location_samples(product.name)
            ],
            _utils.product_license(product),
            url_for("product.raw_product_doc", name=product.name, _external=True),
            _iso8601_duration(summary.last_refresh_age),
            product.metadata_type.name,
        )
        for product, summary in _model.get_products_with_summaries()
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
        "about.html",
        product_summary_and_location=[
            (product, summary, _model.STORE.product_location_samples(product.name))
            for product, summary in _model.get_products_with_summaries()
        ],
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
        location_samples=_model.STORE.product_location_samples(name),
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


def _iso8601_duration(tdelta: timedelta):
    """
    Format a timedelta as an iso8601 duration

    >>> _iso8601_duration(timedelta(seconds=0))
    'PT0S'
    >>> _iso8601_duration(timedelta(seconds=1))
    'PT1S'
    >>> _iso8601_duration(timedelta(seconds=23423))
    'PT6H30M23S'
    >>> _iso8601_duration(timedelta(seconds=4564564556))
    'P52830DT14H35M56S'
    """
    all_secs = tdelta.total_seconds()

    secs = int(all_secs % 60)
    h_m_s = (
        int(all_secs // 3600 % 24),
        int(all_secs // 60 % 60),
        secs if secs % 1 != 0 else int(secs),
    )

    parts = ["P"]

    days = int(all_secs // 86400)
    if days:
        parts.append(f"{days}D")
    if any(h_m_s):
        parts.append("T")
    if all_secs:
        for val, name in zip(h_m_s, ["H", "M", "S"]):
            if val:
                parts.append(f"{val}{name}")
    else:
        parts.append("T0S")

    return "".join(parts)
