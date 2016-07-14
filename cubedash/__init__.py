import collections
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
from flask.ext.compress import Compress

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


# There's probably a proper bottle way to do this.
URL_PREFIX = '/api'


@app.route(URL_PREFIX + '/products')
def get_products():
    types = index.datasets.types.get_all()
    return as_json({type_.name: type_.definition for type_ in types})


@app.route(URL_PREFIX + '/products/<name>')
def get_product(name):
    type_ = index.datasets.types.get_by_name(name)
    return as_json(type_.definition)


@app.route(URL_PREFIX + '/datasets')
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


@app.route(URL_PREFIX + '/datasets/<product>/<int:year>-<int:month>')
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


@app.route(URL_PREFIX + '/timeline/<product>')
def get_product_timeline(product):
    result = []
    for time in month_iter(datetime(2013, 1, 1), datetime.now()):
        count = index.datasets.count(product=product, time=time)
        result.append((time.begin, count))
    return as_json({'data': result})


@app.route(URL_PREFIX + '/datasets/id/<id_>')
def get_dataset(id_):
    dataset_ = index.datasets.get(id_, include_sources=True)
    return as_json(dataset_.metadata_doc)


@app.route('/')
def index_page():
    product = _get_product()
    types = index.datasets.types.get_all()
    return flask.render_template(
        'map.html.jinja2',
        products=[p.definition for p in types],
        selected_product=product
    )


def _get_product():
    if 'product' in flask.request.args:
        product = flask.request.args['product']
    else:
        product = 'ls8_nbar_albers'
    return product


@app.route('/timeline/<product>')
def timeline_page(product):
    types = index.datasets.types.get_all()
    years = _timeline_years(2013, product)
    return flask.render_template(
        'time.html.jinja2',
        year_month_counts=years[0],
        max_count=years[1],
        products=[p.definition for p in types],
        selected_product=product
    )


@app.route('/datasets')
def datasets_page():
    args = flask.request.args
    # Product is mandatory
    product = _get_product()
    query = {'product': product}
    query.update(parse_query(args))
    return flask.render_template(
        'datasets.html.jinja2',
        products=[p.definition for p in (index.datasets.types.get_all())],
        selected_product=product,
        datasets=index.datasets.search(**query),
        query_params=query
    )


@app.route('/datasets/<uuid:id_>')
def dataset_page(id_):
    return flask.render_template(
        'dataset.html.jinja2',
        dataset=(index.datasets.get(str(id_), include_sources=True))
    )


if __name__ == '__main__':
    app.run(port=8080, debug=True)
