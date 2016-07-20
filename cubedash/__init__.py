import collections
import functools
from datetime import datetime
from json import dumps as jsonify

import shapely.geometry
import shapely.ops
from cachetools import cached
from dateutil import parser
from dateutil.relativedelta import relativedelta

import flask
import os
import rasterio.warp
from datacube.index import index_connect
from datacube.model import Range
from datacube.utils import jsonify_document
from flask_compress import Compress

_PRODUCT_PREFIX = '/<product>'
# There's probably a proper flask way to do this.
API_PREFIX = '/api'

index = index_connect()
static_prefix = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'frontend')
app = flask.Flask(__name__, static_path='', static_url_path=static_prefix)
Compress(app)

ACCEPTABLE_SEARCH_FIELDS = ['platform', 'instrument', 'product']


def as_json(o):
    return jsonify(jsonify_document(o), indent=4)


@app.template_filter('strftime')
def _format_datetime(date, fmt=None):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@app.template_filter('query_value')
def _format_query_value(val):
    if type(val) == Range:
        return '{} to {}'.format(_format_query_value(val.begin), _format_query_value(val.end))
    if type(val) == datetime:
        return _format_datetime(val)
    return str(val)


@app.template_filter('month_name')
def _format_month_name(val):
    ds = datetime(2016, int(val), 2)
    return ds.strftime("%b")


def parse_query(request):
    query = {}
    for field in ACCEPTABLE_SEARCH_FIELDS:
        if field in request:
            query[field] = request[field]

    to_time = parser.parse(request['before']) if 'before' in request else None
    from_time = parser.parse(request['after']) if 'after' in request else None

    # Default from/to values (a one month range)
    if not from_time and not to_time:
        to_time = datetime.now()
    if not to_time:
        to_time = from_time + relativedelta(months=1)
    if not from_time:
        from_time = to_time - relativedelta(months=1)

    query['time'] = Range(from_time, to_time)

    def range_dodge(val):
        if isinstance(val, list):
            return Range(val[0], val[1])
        else:
            return Range(val - 0.00005, val + 0.00005)

    if 'lon' in request and 'lat' in request:
        query['lon'] = range_dodge(request['lon'])
        query['lat'] = range_dodge(request['lat'])
    return query


def datasets_union(dss):
    return shapely.ops.unary_union([shapely.geometry.Polygon(ds.extent.points) for ds in dss])


def warp_geometry(geom, crs):
    return rasterio.warp.transform_geom(crs, 'EPSG:4326', geom)


def next_date(date):
    if date.month == 12:
        return datetime(date.year + 1, 1, 1)
    else:
        return datetime(date.year, date.month + 1, 1)


@app.route(API_PREFIX + '/products')
def get_products():
    types = index.datasets.types.get_all()
    return as_json({type_.name: type_.definition for type_ in types})


@app.route(API_PREFIX + '/products/<name>')
def get_product(name):
    type_ = index.datasets.types.get_by_name(name)
    return as_json(type_.definition)


@app.route(API_PREFIX + '/datasets')
def get_datasets():
    return as_json({'error': 'Too many. TODO: paging'})


def dataset_to_feature(ds):
    properties = {
        'id': ds.id,
        'product': ds.type.name,
        'time': ds.center_time
    }
    return {
        'type': 'Feature',
        'geometry': warp_geometry(shapely.geometry.mapping(shapely.geometry.Polygon(ds.extent.points)), str(ds.crs)),
        'properties': {
            'id': ds.id,
            'product': ds.type.name,
            'time': ds.center_time
        }
    }


@app.route(API_PREFIX + '/datasets/<product>/<int:year>-<int:month>')
@cached(cache={})
def datasets_as_features(product, year, month):
    start = datetime(year, month, 1)
    time = Range(start, next_date(start))
    datasets = index.datasets.search(product=product, time=time)
    return as_json({
        'type': 'FeatureCollection',
        'features': [dataset_to_feature(ds) for ds in datasets]
    })


def month_iter(begin, end):
    begin = datetime(begin.year, begin.month, 1)
    while begin < end:
        yield Range(begin, next_date(begin))
        begin = next_date(begin)


@cached(cache={})
def _timeline_years(from_year, product):
    max_value = 0
    years = collections.OrderedDict()
    for time in month_iter(datetime(from_year, 1, 1), datetime.now()):
        count = index.datasets.count(product=product, time=time)
        if max_value < count:
            max_value = count
        if time.begin.year not in years:
            years[time.begin.year] = {}
        years[time.begin.year][time.begin.month] = count
    return years, max_value


@app.route('/')
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(flask.url_for('map_page', product='ls7_level1_scene'))


@app.route('%s/spacial' % _PRODUCT_PREFIX)
def map_page(product):
    types = index.datasets.types.get_all()
    return flask.render_template(
        'spacial.html',
        products=[p.definition for p in types],
        selected_product=product
    )


@app.route('%s/timeline' % _PRODUCT_PREFIX)
def timeline_page(product):
    types = index.datasets.types.get_all()
    years = _timeline_years(datetime.today().year - 15, product)
    return flask.render_template(
        'timeline.html',
        year_month_counts=years[0],
        max_count=years[1],
        products=[p.definition for p in types],
        selected_product=product
    )


@app.route('%s/datasets' % _PRODUCT_PREFIX)
def datasets_page(product):
    args = flask.request.args
    query = {'product': product}
    query.update(parse_query(args))
    return flask.render_template(
        'datasets.html',
        products=[p.definition for p in (index.datasets.types.get_all())],
        selected_product=product,
        datasets=index.datasets.search_eager(**query),
        query_params=query
    )


@app.route('/datasets/<uuid:id_>')
def dataset_page(id_):
    dataset = index.datasets.get(str(id_), include_sources=True)

    ordered_metadata = get_ordered_metadata(dataset.metadata_doc)

    return flask.render_template(
        'dataset.html',
        dataset=dataset,
        dataset_metadata=ordered_metadata
    )


def get_ordered_metadata(metadata_doc):
    def get_property_priority(ordered_properties, keyval):
        key, val = keyval
        if key not in ordered_properties:
            return 999
        return ordered_properties.index(key)

    # Give the document the same order as eo-datasets. It's far more readable (ID/names first, sources last etc.)
    ordered_metadata = collections.OrderedDict(
        sorted(metadata_doc.items(),
               key=functools.partial(get_property_priority, EODATASETS_PROPERTY_ORDER))
    )
    ordered_metadata['lineage'] = collections.OrderedDict(
        sorted(ordered_metadata['lineage'].items(),
               key=functools.partial(get_property_priority, EODATASETS_LINEAGE_PROPERTY_ORDER))
    )

    if 'source_datasets' in ordered_metadata['lineage']:
        for type, source_dataset_doc in ordered_metadata['lineage']['source_datasets'].items():
            ordered_metadata['lineage']['source_datasets'][type] = get_ordered_metadata(source_dataset_doc)

    return ordered_metadata


EODATASETS_PROPERTY_ORDER = ['id', 'ga_label', 'ga_level', 'product_type', 'product_level', 'product_doi',
                             'creation_dt', 'size_bytes', 'checksum_path', 'platform', 'instrument', 'format', 'usgs',
                             'rms_string', 'acquisition', 'extent', 'grid_spatial', 'gqa', 'browse', 'image', 'lineage',
                             'product_flags']
EODATASETS_LINEAGE_PROPERTY_ORDER = ['algorithm', 'machine', 'ancillary_quality', 'ancillary', 'source_datasets']
if __name__ == '__main__':
    app.run(port=8080, debug=True)
