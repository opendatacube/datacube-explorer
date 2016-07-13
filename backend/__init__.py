from datetime import datetime
from json import JSONEncoder
from json import dumps as jsonify

import rasterio.warp
from dateutil import parser
from dateutil.tz import tzutc

from bottle import Bottle, JSONPlugin, post, request, route, run, static_file, template
from datacube.index import index_connect
from datacube.model import Range, _DocReader
from datacube.storage.storage import DatasetSource

index = index_connect()


class MyJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)


app = Bottle(autojson=False)
app.install(JSONPlugin(json_dumps=lambda s: jsonify(s, cls=MyJsonEncoder)))

FIELDS = ["platform", "instrument", "product"]
OFFSETS = {
    "platform": ["platform", "code"],
    "instrument": ["instrument", "name"],
    "product": ["product_type"],
    "start": ["extent", "from_dt"],
    "end": ["extent", "to_dt"],
    "time": ["extent", "center_dt"],
}


def _get_doc_offset(offset, document):
    value = document
    for key in offset:
        value = value[key]
    return value


def format_search(datasets):
    for ds in datasets:
        yield {
            "id": ds.id,
            "collection": ds.collection.name,
            "metadata": ds.metadata_doc,
        }


def format_summary(datasets, time_range):
    bins = [0] * 50

    def time_index(v):
        return int(
            (v - time_range.begin).total_seconds()
            * len(bins)
            / (time_range.end - time_range.begin).total_seconds()
        )

    total = 0
    data = {}
    for dataset in datasets:

        metadata = _DocReader(OFFSETS, dataset.metadata_doc)
        key = metadata.platform, metadata.instrument, metadata.product
        total += 1

        if metadata.product == "satellite_telemetry_data":
            continue

        start_time = parser.parse(metadata.start)
        if not start_time.tzinfo:
            start_time = start_time.replace(tzinfo=tzutc())
        end_time = parser.parse(metadata.end)
        if not end_time.tzinfo:
            end_time = end_time.replace(tzinfo=tzutc())

        for i in range(time_index(start_time), time_index(end_time) + 1):
            bins[i] += 1

        if key in data:
            data[key]["count"] += 1
            data[key]["start_time"] = min(data[key]["start_time"], start_time)
            data[key]["end_time"] = max(data[key]["end_time"], end_time)
            continue

        data[key] = {
            "platform": metadata.platform,
            "instrument": metadata.instrument,
            "product": metadata.product,
            "measurements": [
                {"name": name}
                for name, descr in dataset.metadata.measurements_dict.items()
            ],
            "start_time": start_time,
            "end_time": end_time,
            "count": 1,
        }
    return {"total": total, "datasets": data.values(), "timeline": bins}


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


@app.route("/datacube/api/collections/<id_>")
def collections(id_):
    collection = index.collections.get(int(id_))
    return {"id": collection.id_, "name": collection.name}


@app.post("/datacube/api/search")
def search():
    query = parse_query(request.json)
    datasets = index.datasets.search(**query)
    return {"data": list(format_search(datasets))}


@app.post("/datacube/api/search-summary")
def search_summary():
    query = parse_query(request.json)
    datasets = index.datasets.search(**query)
    return {"data": format_summary(datasets, query["time"])}


@app.route("/datacube/api/datasets/")
def datasets():
    datasets = index.datasets.search()
    return {"data": list(format_search(datasets))}


def get_data_value(source, lat, lon):
    pass


@app.post("/datacube/api/pixel-drill")
def pixel_drill():
    query = parse_query(request.json)
    datasets = index.datasets.search(**query)
    lat, lon = request.json["lat"], request.json["lon"]

    data = {}
    for dataset in datasets:
        metadata = _DocReader(OFFSETS, dataset.metadata_doc)
        key = metadata.platform, metadata.instrument, metadata.product
        results = []
        for idx, measurement in enumerate(request.json["measurements"]):
            source = DatasetSource(dataset, measurement)
            with source.open() as goo:
                (x,), (y,) = rasterio.warp.transform(
                    "EPSG:4326", source.crs, [lon], [lat]
                )
                i, j = (~source.transform) * (x, y)
                # print i,j
                # print x, y
                # print source.transform

                # print source
                x = goo.ds.read(indexes=goo.bidx, window=((j, j + 1), (i, i + 1)))
                # print x
                # print type(x[0,0])
            results.append(x[0, 0].item())
        data.setdefault(key, []).append((parser.parse(metadata.time), results))

    result = []
    for (platform, instrument, product), items in data.items():
        items.sort(key=lambda item: item[0])
        result.append(
            {
                "platform": platform,
                "instrument": instrument,
                "product": product,
                "time": [item[0] for item in items],
                "measurements": {},
            }
        )
        for idx, measurement in enumerate(request.json["measurements"]):
            result[-1]["measurements"][measurement] = [item[1][idx] for item in items]

    return {"data": result}


run(app=app, host="0.0.0.0", port=8080, debug=True)
