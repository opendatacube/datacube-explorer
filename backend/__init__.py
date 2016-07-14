from datetime import datetime
from json import JSONEncoder
from json import dumps as jsonify

import shapely.geometry
import shapely.ops
from dateutil import parser

from bottle import Bottle, JSONPlugin, run
from datacube.index import index_connect
from datacube.model import Range

index = index_connect()


class MyJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)


app = Bottle(autojson=False)
app.install(JSONPlugin(json_dumps=lambda s: jsonify(s, cls=MyJsonEncoder)))

FIELDS = ["platform", "instrument", "product"]


def parse_query(request):
    query = {}
    for field in FIELDS:
        query[field] = request[field]
    query["time"] = Range(
        parser.parse(request["after"]), parser.parse(request["before"])
    )

    def range_dodge(val):
        if isinstance(val, list):
            return Range(val[0], val[1])
        else:
            return Range(val - 0.00005, val + 0.00005)

    if "lon" in request and "lat" in request:
        query["lon"] = range_dodge(request["lon"])
        query["lat"] = range_dodge(request["lat"])
    return query


def datasets_union(dss):
    return shapely.ops.unary_union(
        [shapely.geometry.Polygon(ds.extent.points) for ds in dss]
    )


@app.route("/api/ls8_nbar")
def ls8_nbar():
    product = "ls8_nbar_albers"
    year = 2014
    time = Range(datetime(year, 1, 1), datetime(year, 7, 1))
    scenes = index.datasets.search(product=product, time=time)
    geometry = datasets_union(scenes)
    return shapely.geometry.mapping(geometry)


@app.route("/api/types")
def products():
    types = index.datasets.types.get_all()
    return {type_.name: type_.definition for type_ in types}


@app.route("/api/types/<name>")
def product(name):
    type_ = index.datasets.types.get_by_name(name)
    return type_.definition


run(app=app, host="0.0.0.0", port=8080, debug=True)
