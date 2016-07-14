from datacube.index import index_connect
index = index_connect()

from bottle import route, post, run, template, static_file, request, Bottle, JSONPlugin
from dateutil import parser
from datacube.model import Range
from json import JSONEncoder, dumps as jsonify
from datetime import datetime
import shapely.ops
import shapely.geometry


class MyJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)


app = Bottle(autojson=False)
app.install(JSONPlugin(json_dumps=lambda s: jsonify(s, cls=MyJsonEncoder)))

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
            return Range(val-0.00005, val+0.00005)

    if 'lon' in request and 'lat' in request:
        query['lon'] = range_dodge(request['lon'])
        query['lat'] = range_dodge(request['lat'])
    return query


def datasets_union(dss):
    return shapely.ops.unary_union([shapely.geometry.Polygon(ds.extent.points) for ds in dss])


@app.route('/datacube/ls8_nbar')
def ls8_nbar():
    product = 'ls8_nbar_albers'
    year = 2014
    time = Range(datetime(year, 1, 1), datetime(year, 7, 1))
    scenes = index.datasets.search(product=product, time=time)
    geometry = datasets_union(scenes)
    return shapely.geometry.mapping(geometry)


run(app=app, host='0.0.0.0', port=8080, debug=True)
