import logging
from datetime import datetime

import shapely.geometry
import shapely.ops
from cachetools.func import ttl_cache
from datacube.model import Range
from datacube.utils.geometry import CRS
from flask import Blueprint

from cubedash._model import CACHE_LONG_TIMEOUT_SECS, index, as_json

_LOG = logging.getLogger(__name__)
bp = Blueprint('api', __name__, url_prefix='/api')


def next_date(date):
    if date.month == 12:
        return datetime(date.year + 1, 1, 1)

    return datetime(date.year, date.month + 1, 1)


@bp.route('/datasets/<product>/<int:year>-<int:month>')
@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def datasets_as_features(product, year, month):
    start = datetime(year, month, 1)
    time = Range(start, next_date(start))
    datasets = index.datasets.search(product=product, time=time)
    return as_json({
        'type': 'FeatureCollection',
        'features': [dataset_to_feature(ds)
                     for ds in datasets if ds.extent]
    })


@bp.route('/datasets/<product>/<int:year>-<int:month>/poly')
@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def dataset_shape(product, year, month):
    start = datetime(year, month, 1)
    time = Range(start, next_date(start))
    datasets = index.datasets.search(product=product, time=time)

    dataset_shapes = [shapely.geometry.asShape(ds.extent.to_crs(CRS('EPSG:4326')))
                      for ds in datasets if ds.extent]
    return as_json(dict(
        type='Feature',
        geometry=shapely.ops.unary_union(dataset_shapes).__geo_interface__,
        properties=dict(
            dataset_count=len(dataset_shapes)
        )
    ))


def dataset_to_feature(ds):
    return {
        'type': 'Feature',
        'geometry': ds.extent.to_crs(CRS('EPSG:4326')).__geo_interface__,
        'properties': {
            'id': ds.id,
            'product': ds.type.name,
            'time': ds.center_time
        }
    }
