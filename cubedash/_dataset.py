import logging
from uuid import UUID

import flask
from flask import Blueprint, abort, current_app, url_for

from . import _model
from . import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint(
    "dataset",
    __name__,
)

PROVENANCE_DISPLAY_LIMIT = 25


@bp.route("/dataset/<uuid:id_>")
def dataset_page(id_):
    index = _model.INDEX
    dataset = index.get_ds(id_)

    if dataset is None:
        abort(404, f"No dataset found with id {id_}")

    return flask.redirect(
        url_for(
            "dataset.dataset_full_page",
            product_name=dataset.product.name,
            id_=dataset.id,
        )
    )


@bp.route("/products/<product_name>/datasets/<uuid:id_>")
def dataset_full_page(product_name: str, id_: UUID):
    derived_dataset_overflow = source_dataset_overflow = 0

    index = _model.INDEX
    dataset = index.get_ds(id_, include_sources=False)

    if dataset is None:
        abort(404, f"No dataset found with id {id_}")

    if product_name != dataset.product.name:
        actual_url = url_for(
            "dataset.dataset_full_page",
            product_name=dataset.product.name,
            id_=dataset.id,
        )
        abort(
            404,
            f"No dataset found for product {product_name!r}, "
            f"however one with that id was found in product {product_name!r}. "
            f"Perhaps you meant to visit {actual_url!r}",
        )

    provenance_display_limit = current_app.config.get(
        "CUBEDASH_PROVENANCE_DISPLAY_LIMIT", PROVENANCE_DISPLAY_LIMIT
    )
    source_datasets, source_dataset_overflow = index.get_dataset_sources(
        id_, limit=provenance_display_limit
    )

    dataset.metadata.sources = {}
    ordered_metadata = utils.prepare_dataset_formatting(dataset)

    derived_datasets, derived_dataset_overflow = index.get_datasets_derived(
        id_, limit=provenance_display_limit
    )
    derived_datasets.sort(key=utils.dataset_label)

    footprint, region_code = _model.STORE.get_dataset_footprint_region(id_)
    # We only have a footprint in the spatial table above if summarisation has been
    # run for the product (...and done so after the dataset was added).
    #
    # Fall back to a regular footprint for other datasets.
    if not footprint:
        footprint, is_valid = utils.dataset_shape(dataset)

    return utils.render(
        "dataset.html",
        dataset=dataset,
        dataset_footprint=footprint,
        dataset_region_code=region_code,
        dataset_metadata=ordered_metadata,
        derived_datasets=derived_datasets,
        source_datasets=source_datasets,
        derived_dataset_overflow=derived_dataset_overflow,
        source_dataset_overflow=source_dataset_overflow,
        # get dataset bounds for better image overlay
        dataset_bounds=utils.bbox_as_geom(dataset),
    )


@bp.route("/dataset/<uuid:id_>.odc-metadata.yaml")
def raw_doc(id_):
    index = _model.INDEX
    dataset = index.get_ds(id_, include_sources=True)

    if dataset is None:
        abort(404, f"No dataset found with id {id_}")

    # Format for readability
    return utils.as_yaml(
        utils.prepare_dataset_formatting(
            dataset, include_source_url=True, include_locations=True
        )
    )
