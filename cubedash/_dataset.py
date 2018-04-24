from __future__ import absolute_import

import logging

import flask
from flask import Blueprint

from . import _utils as utils
from ._model import index

_LOG = logging.getLogger(__name__)
bp = Blueprint('dataset', __name__, url_prefix='/dataset')


@bp.route('/<uuid:id_>')
def dataset_page(id_):
    dataset = index.datasets.get(id_, include_sources=True)

    source_datasets = {type_: index.datasets.get(dataset_d['id'])
                       for type_, dataset_d in dataset.metadata.sources.items()}

    archived_location_times = index.datasets.get_archived_location_times(id_)
    ordered_metadata = utils.get_ordered_metadata(dataset.metadata_doc)

    derived_datasets = sorted(index.datasets.get_derived(id_), key=utils.dataset_label)

    return flask.render_template(
        'dataset.html',
        dataset=dataset,
        dataset_metadata=ordered_metadata,
        derived_datasets=derived_datasets,
        source_datasets=source_datasets,
        archive_location_times=archived_location_times
    )
