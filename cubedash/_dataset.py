from __future__ import absolute_import

import logging

import flask
from flask import Blueprint
from jinja2 import Markup

from datacube.model import Dataset

from . import _model
from . import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("dataset", __name__, url_prefix="/dataset")

PROVENANCE_DISPLAY_LIMIT = 50


@bp.route("/<uuid:id_>")
def dataset_page(id_):
    derived_dataset_overflow = source_dataset_overflow = 0

    index = _model.STORE.index
    dataset = index.datasets.get(id_, include_sources=True)

    source_list = list(dataset.metadata.sources.items())
    if len(source_list) > PROVENANCE_DISPLAY_LIMIT:
        source_dataset_overflow = len(source_list) - PROVENANCE_DISPLAY_LIMIT
        source_list = source_list[:PROVENANCE_DISPLAY_LIMIT]

    source_datasets = {
        type_: index.datasets.get(dataset_d["id"]) for type_, dataset_d in source_list
    }

    archived_location_times = index.datasets.get_archived_location_times(id_)

    # simple_sources = {classifier: dataset_link(d) for classifier, d in dataset.sources.items()}
    dataset.metadata.sources = {}
    ordered_metadata = utils.get_ordered_metadata(dataset.metadata_doc)

    derived_datasets = sorted(index.datasets.get_derived(id_), key=utils.dataset_label)
    if len(derived_datasets) > PROVENANCE_DISPLAY_LIMIT:
        derived_dataset_overflow = len(derived_datasets) - PROVENANCE_DISPLAY_LIMIT
        derived_datasets = derived_datasets[:PROVENANCE_DISPLAY_LIMIT]

    return flask.render_template(
        "dataset.html",
        dataset=dataset,
        dataset_metadata=ordered_metadata,
        derived_datasets=derived_datasets,
        source_datasets=source_datasets,
        archive_location_times=archived_location_times,
        derived_dataset_overflow=derived_dataset_overflow,
        source_dataset_overflow=source_dataset_overflow,
    )


def dataset_link(d: Dataset):
    return Markup(
        f'<a href="{flask.url_for("dataset.dataset_page", id_=d.id)}">{d.id}</a>'
    )
