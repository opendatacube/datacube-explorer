import itertools

import json
import logging
from datetime import datetime, timedelta, time as dt_time
from flask import Blueprint, request, abort, url_for
from typing import Tuple

from cubedash.summary._stores import DatasetItem
from datacube.utils import parse_time
from . import _model
from . import _utils

_LOG = logging.getLogger(__name__)
bp = Blueprint('stac', __name__, url_prefix='/stac')

DATASET_LIMIT = 100
DEFAULT_PAGE_SIZE = 20


@bp.route('/')
def root():
    return abort(404, "Only /stac/search is currently supported")


@bp.route('/search', methods=['GET', 'POST'])
def stac_search():
    if request.method == 'GET':
        bbox = request.args.get('bbox')
        bbox = json.loads(bbox)
        time_ = request.args.get('time')
        product_name = request.args.get('product')
        limit = request.args.get('limit', default=DEFAULT_PAGE_SIZE, type=int)
        offset = request.args.get('offset', default=0, type=int)
    else:
        req_data = request.get_json()
        bbox = req_data.get('bbox')
        time_ = req_data.get('time')
        product_name = req_data.get('product')
        limit = req_data.get('limit') or DEFAULT_PAGE_SIZE
        offset = req_data.get('offset') or 0

    # bbox and time are compulsory
    if not bbox:
        abort(400, "bbox must be specified")
    if not time_:
        abort(400, "time must be specified")

    if offset >= DATASET_LIMIT:
        abort(
            400,
            "Server paging limit reached (first {} only)".format(DATASET_LIMIT)
        )
    # If the request goes past MAX_DATASETS, shrink the limit to match it.
    if (offset + limit) > DATASET_LIMIT:
        limit = (DATASET_LIMIT - offset)
        # TODO: mention in the reply that we've hit a limit?

    if len(bbox) != 4:
        abort(400, "Expected bbox of size 4. [min lon, min lat, max long, max lat]")

    time_ = _parse_time_range(time_)

    return _utils.as_json(
        search_datasets_stac(
            product_name=product_name,
            bbox=bbox,
            time=time_,
            limit=limit,
            offset=offset,
        )
    )


def search_datasets_stac(
        product_name: str,
        bbox: Tuple[float, float, float, float],
        time: Tuple[datetime, datetime],
        limit: int,
        offset: int,
):
    """
    Returns a GeoJson FeatureCollection corresponding to given parameters for
    a set of datasets returned by datacube.
    """
    offset = offset or 0
    end_offset = offset + limit

    items = list(_model.STORE.get_dataset_footprints(
        product_name=product_name,
        time=time,
        bbox=bbox,
        limit=limit + 1,
        offset=offset,
        full_dataset=True,
    ))

    result = dict(
        type='FeatureCollection',
        features=[as_stac_item(f) for f in items[:limit]],
        meta=dict(
            page=offset // limit,
            limit=limit,
        ),
        links=[]
    )

    there_are_more = len(items) == limit + 1

    if there_are_more and end_offset < DATASET_LIMIT:
        result['links'].append(dict(
            rel='next',
            href=url_for(
                '.stac_search',
                product=product_name,
                bbox='[{},{},{},{}]'.format(*bbox),
                time=_unparse_time_range(time),
                limit=limit,
                offset=end_offset,
            )
        ))

    return result


def _parse_time_range(time: str) -> Tuple[datetime, datetime]:
    """
    >>> _parse_time_range('1986-04-16T01:12:16/2097-05-10T00:24:21')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(2097, 5, 10, 0, 24, 21))
    >>> _parse_time_range('1986-04-16T01:12:16')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(1986, 4, 16, 1, 12, 17))
    >>> _parse_time_range('1986-04-16')
    (datetime.datetime(1986, 4, 16, 0, 0), datetime.datetime(1986, 4, 17, 0, 0))
    """
    time_period = time.split('/')
    if len(time_period) == 2:
        return parse_time(time_period[0]), parse_time(time_period[1])
    elif len(time_period) == 1:
        t: datetime = parse_time(time_period[0])
        if t.time() == dt_time():
            return t, t + timedelta(days=1)
        else:
            return t, t + timedelta(seconds=1)


def _unparse_time_range(time: Tuple[datetime, datetime]) -> str:
    """
    >>> _unparse_time_range((
    ...     datetime(1986, 4, 16, 1, 12, 16),
    ...     datetime(2097, 5, 10, 0, 24, 21)
    ... ))
    '1986-04-16T01:12:16/2097-05-10T00:24:21'
    """
    start_time, end_time = time
    return f"{start_time.isoformat()}/{end_time.isoformat()}"


def as_stac_item(dataset: DatasetItem):
    """
    Returns a dict corresponding to a stac item
    """
    item = dict(
        id=dataset.id,
        type='Feature',
        bbox=dataset.bbox,
        geometry=dataset.geom_geojson,
        properties={
            'datetime': dataset.center_time,
            'odc:product': dataset.product_name,
            'odc:creation-time': dataset.creation_time,
            'cubedash:region_code': dataset.region_code,
        },
        links=[],
    )

    if dataset.full_dataset:
        item['assets'] = {
            band_name: dict(
                href=band_data['path']
            ) for band_name, band_data in dataset.full_dataset.measurements.items()
        }

    return item
