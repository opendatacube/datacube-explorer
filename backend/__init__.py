from datacube.index import index_connect

index = index_connect()

from bottle import run, Bottle, JSONPlugin
from dateutil import parser
from datacube.model import Range
from datacube.utils import jsonify_document
from json import JSONEncoder, dumps as jsonify
from datetime import datetime
import shapely.ops
import shapely.geometry


app = Bottle(autojson=False)
app.install(JSONPlugin(json_dumps=lambda s: jsonify(jsonify_document(s))))

FIELDS = ['platform', 'instrument', 'product']


def parse_query(request):
    query = {}
    for field in FIELDS:
        query[field] = request[field]
    query['time'] = Range(parser.parse(request['after']), parser.parse(request['before']))

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


# There's probably a proper bottle way to do this.
URL_PREFIX = '/api'


@app.route(URL_PREFIX + '/types')
def products():
    types = index.datasets.types.get_all()
    return {type_.name: type_.definition for type_ in types}


@app.route(URL_PREFIX + '/types/<name>')
def product(name):
    type_ = index.datasets.types.get_by_name(name)
    return type_.definition


@app.route(URL_PREFIX + '/datasets')
def datasets():
    return {'error': 'Too many. TODO: paging'}


@app.route(URL_PREFIX + '/datasets/ls8_nbar')
def ls8_nbar():
    product = 'ls8_nbar_albers'
    year = 2014
    time = Range(datetime(year, 1, 1), datetime(year, 7, 1))
    scenes = index.datasets.search(product=product, time=time)
    geometry = datasets_union(scenes)
    return shapely.geometry.mapping(geometry)


@app.route(URL_PREFIX + '/datasets/id/<id_>')
def product(id_):
    dataset_ = index.datasets.get(id_)
    return dataset_.metadata_doc


run(app=app, host='0.0.0.0', port=8080, debug=True)
