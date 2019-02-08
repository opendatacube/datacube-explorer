import logging
from typing import Dict, List

from dateutil import tz
from flask import Blueprint, request, url_for

from datacube.model import DatasetType
from . import _model
from . import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint('stac', __name__, url_prefix='/stac')

_STAC_DEFAULTS = dict(
    stac_version="0.6.0",
)


@bp.route('/')
def root():
    return utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id="sat-api",
            title="sat-api for public datasets",
            description="sat-api for public datasets by Development Seed",
            links=[
                *(
                    dict(
                        rel="child",
                        href=url_for('stac.collection', product_name=product.name),
                        title=product.metadata_doc.get('description')
                    )
                    for product, product_summary in _model.get_products_with_summaries()
                ),
                dict(
                    rel="self",
                    href=request.url
                )
            ]
        )
    )


@bp.route('/collections/<product_name>')
def collection(product_name: str):
    summary = _model.get_product_summary(product_name)
    dataset_type = _model.STORE.get_dataset_type(product_name)
    all_time_summary = _model.get_time_summary(product_name)

    begin, end = _time_range_utc(all_time_summary)
    return utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=summary.name,
            title=dataset_type.metadata_doc.get('description'),
            properties=dict(_build_properties(dataset_type)),
            extent=dict(
                spatial=all_time_summary.footprint_wrs84.bounds,
                temporal=[begin, end],
            ),
        )
    )


def field_platform(value):
    return "eo:platform", value.lower().replace("_", "-")


def field_instrument(value):
    return "eo:instrument", value


def field_bands(value: List[Dict]):
    return "eo:bands", [dict(name=v['name']) for v in value]


_STAC_PROPERTY_MAP = {
    "platform": field_platform,
    "instrument": field_instrument,
    "measurements": field_bands,
}


def _build_properties(dt: DatasetType):
    for key, val in dt.metadata.fields.items():
        converter = _STAC_PROPERTY_MAP.get(key)
        if converter:
            yield converter(val)


def _time_range_utc(all_time_summary):
    begin, end = all_time_summary.time_range
    return begin.astimezone(tz.tzutc()), end.astimezone(tz.tzutc())
