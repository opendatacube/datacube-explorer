import ciso8601

import logging
from collections import OrderedDict

import warnings
from datetime import datetime
from dateutil import tz
from flask import request, url_for, abort
from pprint import pprint
from typing import Dict, List, Iterable

from cubedash import _utils
from cubedash.summary._stores import ProductSummary
from datacube.model import DatasetType, Dataset, Range
from datacube.utils.uris import pick_uri, uri_resolve
from . import _model, _stac
from . import _utils as utils

_PAGE_SIZE = 50

_LOG = logging.getLogger(__name__)

bp = _stac.bp

_STAC_DEFAULTS = dict(
    stac_version="0.6.0",
)

endpoint_id = 'dea'
endpoint_title = ""
endpoint_description = ""


@bp.route('/')
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
                        title=product.name,
                        description=product.definition.get('description'),
                        href=url_for(
                            'stac.collection',
                            product_name=product.name,
                            _external=True
                        ),
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

    summary_props = {}
    if summary and summary.time_earliest:
        begin, end = _utc(summary.time_earliest), _utc(summary.time_latest)
        extent = {'temporal': [begin, end]}
        footprint = all_time_summary.footprint_wrs84
        if footprint:
            extent['spatial'] = footprint.bounds

        summary_props['extent'] = extent
    return utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=summary.name,
            title=summary.name,
            description=dataset_type.definition.get('description'),
            properties=dict(_build_properties(dataset_type)),
            providers=[],
            **summary_props,
            links=[
                dict(
                    rel='items',
                    href=url_for(
                        'stac.items',
                        product_name=product_name,
                        _external=True,
                    )
                )
            ]
        )
    )


# @bp.route('/search', methods=['GET', 'POST'])
def stac_search():
    if request.method == 'GET':
        bbox = request.args.get('bbox')
        time_ = request.args.get('time')
        product = request.args.get('product')
        limit = request.args.get('limit')
        from_dts = request.args.get('from')
    else:
        req_data = request.get_json()
        bbox = req_data.get('bbox')
        time_ = req_data.get('time')
        product = req_data.get('product')
        limit = req_data.get('limit')
        from_dts = req_data.get('from')

    if not limit or (limit > _PAGE_SIZE):
        limit = _PAGE_SIZE

    return _as_feature_collection(
        load_datasets(bbox, product, time_, limit),
        None, limit=limit
    )


def load_datasets(bbox, product, time, limit) -> Iterable[Dataset]:
    """
    Parse the query parameters and load and return the matching datasets. bbox is assumed to be
    [minimum longitude, minimum latitude, maximum longitude, maximum latitude]
    """

    query = dict()
    if product:
        query['product'] = product

    if time:
        time_period = time.split('/')
        query['time'] = Range(ciso8601.parse_datetime(time_period[0]),
                              ciso8601.parse_datetime(time_period[1]))

    if bbox:
        # bbox is in GeoJSON CRS (WGS84)
        query['lon'] = Range(bbox[0], bbox[2])
        query['lat'] = Range(bbox[1], bbox[3])

    return _model.STORE.index.datasets.search(limit=limit, **query)



@bp.route('/collections/<product_name>/items')
def items(product_name: str):
    return item_list(product_name)


def item_list(product_name: str):
    # Eg. https://sat-api.developmentseed.org/collections/landsat-8-l1/items
    datasets = _model.STORE.index.datasets.search(product=product_name,
                                                  limit=_PAGE_SIZE)
    all_time_summary = _model.get_time_summary(product_name)

    # We maybe shouldn't include "found" as it prevents some future optimisation?
    total_count = all_time_summary.dataset_count

    return _as_feature_collection(datasets, total_count)


def _as_feature_collection(datasets, total_count, limit=_PAGE_SIZE):
    extras = {}
    if total_count:
        extras['found'] = total_count
    return utils.as_json(
        dict(
            meta=dict(
                page=1,
                limit=limit,
                # returned=?
                **extras,
            ),
            type='FeatureCollection',
            features=(stac_item(d) for d in datasets)
        )
    )


@bp.route('/collections/<product_name>/items/<dataset_id>')
def item(product_name, dataset_id):
    dataset = _model.STORE.index.datasets.get(dataset_id)
    if not dataset:
        abort(404, "No such dataset")

    actual_product_name = dataset.type.name
    if product_name != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're jerks)
        actual_url = url_for(
            'stac.item',
            product_name=product_name,
            dataset_id=dataset_id,
            _external=True
        )
        abort(
            404,
            f"No such dataset in collection.\n"
            f"Perhaps you meant collection {actual_product_name}: {actual_url})"
        )

    return utils.as_json(
        stac_item(dataset)
    )


def pick_remote_uri(dataset: Dataset):
    # Return first uri with a remote path (newer paths come first)
    for uri in dataset.uris:
        scheme, *_ = uri.split(':')
        if scheme in ('https', 'http', 'ftp', 's3', 'gfs'):
            return uri

    return None


def stac_item(ds: Dataset):
    shape, valid_extent = _utils.dataset_shape(ds)
    base_uri = pick_remote_uri(ds) or ds.local_uri

    if not shape:
        warnings.warn(f"shapeless dataset of type {ds.type.name}")
        return None

    # Band order needs to be stable.
    bands = enumerate(sorted(ds.measurements.items()))
    # Find all bands without paths. Create base_path asset with all of those eo:bands
    # Remaining bands have their own assets.

    item = OrderedDict([
        ('id', ds.id),
        ('type', 'Feature'),
        ('bbox', shape.bounds),
        ('geometry', shape.__geo_interface__),
        ('properties', {
            'datetime': ds.center_time,
            # TODO: correct?
            'collection': ds.type.name,
            # 'provider': CFG['contact']['name'],
            # 'license': CFG['license']['name'],
            # 'copyright': CFG['license']['copyright'],
            # 'product_type': metadata_doc['product_type'],
            # 'homepage': CFG['homepage']
        }),
        ('links', [
            {
                'rel': 'self',
                'href': url_for('stac.item',
                                product_name=ds.type.name,
                                dataset_id=ds.id),
            },
            {
                'rel': 'parent',
                'href': url_for('stac.collection',
                                product_name=ds.type.name),
            }
        ]),
        ('assets', [
            # TODO ??? rel?
            {'rel': 'base_uri', 'href': base_uri},
            {
                band_name: {} for band_name, band_data in ds.measurements.items()
            }
        ]),
        ('eo:bands', {
            band_name: {
                # "type"? "GeoTIFF" or image/vnd.stac.geotiff; cloud-optimized=true
                'href': uri_resolve(base_uri, band_data.get('path')),
                # "required": 'true',
                # "type": "GeoTIFF"
            }
            for band_name, band_data in ds.measurements.items()
        })
    ])

    # If the dataset has a real start/end time, add it.
    time = ds.time
    if time.begin < time.end:
        item['properties']['dtr:start_datetime'] = _utc(time.begin)
        item['properties']['dtr:end_datetime'] = _utc(time.end)

    return item


def field_platform(value):
    return "eo:platform", value.lower().replace("_", "-")


def field_instrument(value):
    return "eo:instrument", value


def field_bands(value: List[Dict]):
    return "eo:bands", [dict(name=v['name']) for v in value]


def field_path_row(value):
    # eo:row	"135"
    # eo:column	"044"
    pass


# Properties:
# collection	"landsat-8-l1"
# eo:gsd	15
# eo:platform	"landsat-8"
# eo:instrument	"OLI_TIRS"
# eo:off_nadir	0
# datetime	"2019-02-12T19:26:08.449265+00:00"
# eo:sun_azimuth	-172.29462212
# eo:sun_elevation	-6.62176054
# eo:cloud_cover	-1
# eo:row	"135"
# eo:column	"044"
# landsat:product_id	"LC08_L1GT_044135_20190212_20190212_01_RT"
# landsat:scene_id	"LC80441352019043LGN00"
# landsat:processing_level	"L1GT"
# landsat:tier	"RT"

_STAC_PROPERTY_MAP = {
    "platform": field_platform,
    "instrument": field_instrument,
    "measurements": field_bands,
}


def _build_properties(dt: DatasetType):
    for key, val in dt.metadata.fields.items():
        if val is None:
            continue
        converter = _STAC_PROPERTY_MAP.get(key)
        if converter:
            yield converter(val)


def _utc(d: datetime):
    if d is None:
        return None
    return d.astimezone(tz.tzutc())
