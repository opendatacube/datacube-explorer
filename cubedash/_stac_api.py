import logging
from collections import OrderedDict
from typing import Dict, Iterable, List

from dateutil import tz
from flask import abort, request, url_for

from cubedash import _utils
from cubedash.summary._stores import ProductSummary
from datacube.model import Dataset, DatasetType
from datacube.utils.uris import pick_uri, uri_resolve

from . import _model, _stac
from . import _utils as utils

_PAGE_SIZE = 10

_LOG = logging.getLogger(__name__)

bp = _stac.bp

_STAC_DEFAULTS = dict(stac_version="0.6.0")

endpoint_id = "dea"
endpoint_title = ""
endpoint_description = ""


@bp.route("/")
def root():
    return utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=endpoint_id,
            title=endpoint_title,
            description=endpoint_description,
            links=[
                *(
                    dict(
                        rel="child",
                        href=url_for(
                            "stac.collection", product_name=product.name, _external=True
                        ),
                        title=product.metadata_doc.get("description"),
                    )
                    for product, product_summary in _model.get_products_with_summaries()
                ),
                dict(rel="self", href=request.url),
            ],
        )
    )


@bp.route("/collections/<product_name>")
def collection(product_name: str):
    summary = _model.get_product_summary(product_name)
    dataset_type = _model.STORE.get_dataset_type(product_name)
    all_time_summary = _model.get_time_summary(product_name)

    begin, end = _time_range_utc(summary)
    return utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=summary.name,
            title=dataset_type.metadata_doc.get("description"),
            properties=dict(_build_properties(dataset_type)),
            providers=[],
            extent=dict(
                spatial=all_time_summary.footprint_wrs84.bounds, temporal=[begin, end]
            ),
            links=[
                dict(
                    rel="items",
                    href=url_for(
                        "stac.items", product_name=product_name, _external=True
                    ),
                )
            ],
        )
    )


@bp.route("/collections/<product_name>/items")
def items(product_name: str):
    return item_list(product_name)


def item_list(product_name: str):
    # Eg. https://sat-api.developmentseed.org/collections/landsat-8-l1/items
    datasets = _model.STORE.index.datasets.search(
        product=product_name, limit=_PAGE_SIZE
    )
    all_time_summary = _model.get_time_summary(product_name)
    return utils.as_json(
        dict(
            meta=dict(
                page=1,
                limit=_PAGE_SIZE,
                # returned=?
                # We maybe shouldn't include "found" as it prevents some future optimisation?
                found=all_time_summary.dataset_count,
            ),
            type="FeatureCollection",
            features=(stac_item(d) for d in datasets),
        )
    )


@bp.route("/collections/<product_name>/items/<dataset_id>")
def item(product_name, dataset_id):
    dataset = _model.STORE.index.datasets.get(dataset_id)
    if not dataset:
        abort(404, "No such dataset")

    actual_product_name = dataset.type.name
    if product_name != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're jerks)
        actual_url = url_for(
            "stac.item",
            product_name=product_name,
            dataset_id=dataset_id,
            _external=True,
        )
        abort(
            404,
            f"No such dataset in collection.\n"
            f"Perhaps you meant collection {actual_product_name}: {actual_url})",
        )

    return utils.as_json(stac_item(dataset))


def pick_remote_uri(dataset: Dataset):
    # Return first uri with a remote path (newer paths come first)
    for uri in dataset.uris:
        scheme, *_ = uri.split(":")
        if scheme in ("https", "http", "ftp", "s3", "gfs"):
            return uri

    return None


def stac_item(ds: Dataset):
    shape, valid_extent = _utils.dataset_shape(ds)
    base_uri = pick_remote_uri(ds) or ds.local_uri

    # Band order needs to be stable.
    bands = enumerate(sorted(ds.measurements.items()))
    # Find all bands without paths. Create base_path asset with all of those eo:bands
    # Remaining bands have their own assets.

    item = OrderedDict(
        [
            ("id", ds.id),
            ("type", "Feature"),
            ("bbox", shape.bounds),
            ("geometry", shape.__geo_interface__),
            (
                "properties",
                {
                    "datetime": ds.center_time,
                    # 'provider': CFG['contact']['name'],
                    # 'license': CFG['license']['name'],
                    # 'copyright': CFG['license']['copyright'],
                    # 'product_type': metadata_doc['product_type'],
                    # 'homepage': CFG['homepage']
                },
            ),
            (
                "links",
                [
                    {
                        "href": url_for(
                            "stac.item", product_name=ds.type.name, dataset_id=ds.id
                        ),
                        "rel": "self",
                    },
                    {
                        "href": url_for("stac.collection", product_name=ds.type.name),
                        "rel": "parent",
                    },
                ],
            ),
            (
                "assets",
                [
                    # TODO ??? rel?
                    {"rel": "base_uri", "href": base_uri},
                    {band_name: {} for band_name, band_data in ds.measurements.items()},
                ],
            ),
            (
                "eo:bands",
                {
                    band_name: {
                        # "type"? "GeoTIFF" or image/vnd.stac.geotiff; cloud-optimized=true
                        "href": uri_resolve(base_uri, band_data.get("path")),
                        # "required": 'true',
                        # "type": "GeoTIFF"
                    }
                    for band_name, band_data in ds.measurements.items()
                },
            ),
        ]
    )

    # If the dataset has a real start/end time, add it.
    time = ds.time
    if time.begin < time.end:
        item["properties"]["dtr:start_datetime"] = time.begin
        item["properties"]["dtr:end_datetime"] = time.end

    return item


def field_platform(value):
    return "eo:platform", value.lower().replace("_", "-")


def field_instrument(value):
    return "eo:instrument", value


def field_bands(value: List[Dict]):
    return "eo:bands", [dict(name=v["name"]) for v in value]


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


def _time_range_utc(summary: ProductSummary):
    return (
        summary.time_earliest.astimezone(tz.tzutc()),
        summary.time_latest.astimezone(tz.tzutc()),
    )
