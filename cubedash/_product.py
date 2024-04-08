import logging
from datetime import timedelta

from flask import Blueprint, Response, abort, redirect, url_for

from cubedash import _model, _utils
from cubedash import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("product", __name__)


@bp.route("/about.csv")
def legacy_about_csv():
    return redirect(".storage_csv")


@bp.route("/audit/storage.csv")
def storage_csv():
    """Get the product storage table as a CSV"""

    product_locations = _model.STORE.products_location_samples_all()
    return utils.as_csv(
        filename_prefix="product-information",
        headers=(
            "name",
            "count",
            "locations",
            "license",
            "definition",
            "summary_time",
            "metadata_type",
        ),
        rows=(
            (
                product.name,
                summary.dataset_count,
                [
                    location.common_prefix
                    for location in (product_locations.get(product.name) or [])
                ],
                _utils.product_license(product),
                url_for("product.raw_product_doc", name=product.name, _external=True),
                summary.last_refresh_time,
                product.metadata_type.name,
            )
            for product, summary in _model.get_products_with_summaries()
        ),
    )


@bp.route("/products.txt")
def product_list_text():
    # This is useful for bash scripts when we want to loop products :)
    return Response(
        "\n".join(t.name for t in _model.STORE.all_dataset_types()),
        content_type="text/plain",
    )


@bp.route("/metadata-types.txt")
def metadata_type_list_text():
    # This is useful for bash scripts when we want to loop them :)
    return Response(
        "\n".join(t.name for t in _model.STORE.all_metadata_types()),
        content_type="text/plain",
    )


@bp.route("/audit/storage")
def storage_page():
    product_locations = _model.STORE.products_location_samples_all()

    return utils.render(
        "storage.html",
        product_summary_and_location=[
            (product, summary, (product_locations.get(product.name) or []))
            for product, summary in _model.get_products_with_summaries()
        ],
    )


@bp.route("/product")
def product_redirect():
    """
    If people remove the name from a "/product/<name>" url, take them somewhere useful
    """
    return redirect(url_for(".products_page"))


@bp.route("/products")
def products_page():
    return utils.render(
        "products.html",
    )


@bp.route("/metadata-types")
def metadata_types_page():
    return utils.render(
        "metadata-types.html",
    )


@bp.route("/product/<name>.odc-product.yaml")
def legacy_raw_product_doc(name):
    return redirect(url_for(".raw_product_doc", name=name))


@bp.route("/products/<name>.odc-product.yaml")
def raw_product_doc(name):
    product = _model.STORE.index.products.get_by_name(name)
    if not product:
        abort(404, f"Unknown product {name!r}")

    ordered_metadata = utils.prepare_document_formatting(
        product.definition, "Product", include_source_url=True
    )
    return utils.as_yaml(ordered_metadata)


@bp.route("/metadata-type/<name>")
def legacy_metadata_type_page(name):
    return redirect(url_for(".metadata_type_page", name=name))


@bp.route("/metadata-types/<name>")
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
def legacy_metadata_type_doc(name):
    return redirect(url_for(".raw_metadata_type_doc", name=name))


@bp.route("/metadata-types/<name>.odc-type.yaml")
def raw_metadata_type_doc(name):
    metadata_type = _model.STORE.index.metadata_types.get_by_name(name)
    if not metadata_type:
        abort(404, f"Unknown metadata type {name!r}")
    ordered_metadata = utils.prepare_document_formatting(
        metadata_type.definition, "Metadata Type", include_source_url=True
    )
    return utils.as_yaml(ordered_metadata)


@bp.route("/products.odc-product.yaml")
def raw_all_products_doc():
    resp = utils.as_yaml(
        *(
            utils.prepare_document_formatting(
                product.definition,
                f"Product {product.name}",
                include_source_url=url_for(
                    ".raw_product_doc", name=product.name, _external=True
                ),
            )
            for product in _model.STORE.all_dataset_types()
        )
    )
    # Add Explorer ID to the download filename if they have one.
    utils.suggest_download_filename(
        resp,
        prefix="products",
        suffix=".odc-product.yaml",
    )

    return resp


@bp.route("/metadata-types.odc-type.yaml")
def raw_all_metadata_types_doc():
    resp = utils.as_yaml(
        *(
            utils.prepare_document_formatting(
                type_.definition,
                f"Metadata Type {type_.name}",
                include_source_url=url_for(
                    ".raw_metadata_type_doc", name=type_.name, _external=True
                ),
            )
            for type_ in _model.STORE.all_metadata_types()
        ),
    )
    # Add Explorer ID to the download filename if they have one.
    utils.suggest_download_filename(
        resp,
        prefix="metadata-types",
        suffix=".odc-type.yaml",
    )
    return resp


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
