import logging

from flask import Blueprint

from datacube.utils.geometry import CRS
from ._model import get_summary,get_datasets_geojson
from ._utils import as_json

_LOG = logging.getLogger(__name__)
bp = Blueprint('api', __name__, url_prefix='/api')


@bp.route('/datasets/<product>/<int:year>-<int:month>-<int:day>')
def datasets_as_features(product: str, year: int, month: int, day: int):
    return as_json(get_datasets_geojson(product, year, month, day))


@bp.route('/datasets/<product>/<int:year>-<int:month>/poly')
def dataset_shape(product: str, year: int, month: int):
    summary = get_summary(product, year, month)

    return as_json(dict(
        type='Feature',
        geometry=summary.footprint_geometry.__geo_interface__,
        properties=dict(
            dataset_count=summary.footprint_count
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
