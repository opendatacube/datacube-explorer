from __future__ import absolute_import

import logging

from flask import Blueprint, abort

import eodatasets3.serialise
from datacube.index.eo3 import is_doc_eo3
from . import _model
from . import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("dataset", __name__, url_prefix="/dataset")

PROVENANCE_DISPLAY_LIMIT = _model.app.config.get(
    "CUBEDASH_PROVENANCE_DISPLAY_LIMIT", 25
)


@bp.route("/<uuid:id_>")
def dataset_page(id_):
    derived_dataset_overflow = source_dataset_overflow = 0

    index = _model.STORE.index
    dataset = index.datasets.get(id_, include_sources=True)

    if dataset is None:
        abort(404, f"No dataset found with id {id_}")

    source_list = list(dataset.metadata.sources.items())
    if len(source_list) > PROVENANCE_DISPLAY_LIMIT:
        source_dataset_overflow = len(source_list) - PROVENANCE_DISPLAY_LIMIT
        source_list = source_list[:PROVENANCE_DISPLAY_LIMIT]

    source_datasets = {
        type_: index.datasets.get(dataset_d["id"]) for type_, dataset_d in source_list
    }

    archived_location_times = index.datasets.get_archived_location_times(id_)

    dataset.metadata.sources = {}
    ordered_metadata = utils.get_ordered_metadata(dataset.metadata_doc)

    derived_datasets = sorted(index.datasets.get_derived(id_), key=utils.dataset_label)
    if len(derived_datasets) > PROVENANCE_DISPLAY_LIMIT:
        derived_dataset_overflow = len(derived_datasets) - PROVENANCE_DISPLAY_LIMIT
        derived_datasets = derived_datasets[:PROVENANCE_DISPLAY_LIMIT]

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
        archive_location_times=archived_location_times,
        derived_dataset_overflow=derived_dataset_overflow,
        source_dataset_overflow=source_dataset_overflow,
    )


@bp.route("/<uuid:id_>.odc-metadata.yaml")
def raw_doc(id_):
    index = _model.STORE.index
    dataset = index.datasets.get(id_, include_sources=True)

    if dataset is None:
        abort(404, f"No dataset found with id {id_}")

    doc = dataset.metadata_doc
    # Format for readability
    if is_doc_eo3(doc):
        doc = eodatasets3.serialise.format_doc(doc)
        # TODO: Strip EO-legacy fields. Fix sources. Use eodatasets for eo3 formatting?
    else:
        doc = utils.get_ordered_metadata(doc)
    return utils.as_yaml(doc)
