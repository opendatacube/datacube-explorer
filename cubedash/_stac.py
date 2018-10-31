import datetime
import itertools
import json
import logging
from collections import OrderedDict
from functools import reduce
from urllib.parse import urlparse

import flask
from flask import Blueprint, abort, request, url_for

from cubedash import _utils
from datacube.model import Range
from datacube.utils import parse_time
from datacube.utils.geometry import CRS, Geometry

from . import _model
from . import _utils as utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("stac", __name__, url_prefix="/stac")

MAX_DATASETS = 100
DATASETS_PER_REQUEST = 20


@bp.route("/")
def root():
    return abort(404, "Only /stac/search is currently supported")


@bp.route("/search", methods=["GET", "POST"])
def stac_search():
    if request.method == "GET":
        bbox = request.args.get("bbox")
        time_ = request.args.get("time")
        product = request.args.get("product")
        limit = request.args.get("limit")
        from_dts = request.args.get("from")
    else:
        req_data = request.get_json()
        bbox = req_data.get("bbox")
        time_ = req_data.get("time")
        product = req_data.get("product")
        limit = req_data.get("limit")
        from_dts = req_data.get("from")

    # bbox and time are compulsory
    if not bbox:
        abort(400, "bbox must be specified")
    if not time_:
        abort(400, "time must be specified")

    # Verify and cast data types of request data
    bbox = json.loads(bbox) if isinstance(bbox, str) else bbox
    time_ = time_ if isinstance(time_, str) else json.dumps(time_)
    limit = int(limit) if isinstance(limit, str) else limit
    if from_dts and isinstance(from_dts, str):
        from_dts = int(from_dts)

    # from_dts must be lower than limit
    limit = DATASETS_PER_REQUEST if not limit else min(limit, DATASETS_PER_REQUEST)

    if from_dts and from_dts >= MAX_DATASETS:
        abort(
            400, "The parameter from must be lower than (MAX) {}".format(MAX_DATASETS)
        )

    return utils.as_json(search_datasets_stac(product, bbox, time_, limit, from_dts))


def search_datasets_stac(product, bbox, time, limit, from_dts):
    """
    Returns a GeoJson FeatureCollection corresponding to given parameters for
    a set of datasets returned by datacube.
    """

    stac_datasets = stac_datasets_validated(load_datasets(bbox, product, time))

    from_dts_ = from_dts or 0
    to_dts = min(MAX_DATASETS, from_dts_ + limit)
    stac_datasets_ = list(itertools.islice(stac_datasets, from_dts_, to_dts))

    result = dict()
    result["type"] = "FeatureCollection"
    res_bbox = _compute_bbox(stac_datasets_)
    if res_bbox:
        result["bbox"] = res_bbox

    result["features"] = stac_datasets_

    # Check whether stac_datasets has more datasets and we don't want to process
    # more than MAX_DATASETS
    if _generator_not_empty(stac_datasets) and to_dts < MAX_DATASETS:
        url_next = url_for(".stac_search") + "?"
        if product:
            url_next += "product=" + product
        url_next += "&bbox=" + "[{},{},{},{}]".format(*bbox) + "&time=" + time
        url_next += "&limit=" + str(limit)
        url_next += "&from=" + str(to_dts)
        result["links"] = [{"href": url_next, "rel": "next"}]
    return result


def _generator_not_empty(items):
    """
    Check whether the given generator is not empty. Note: this function can remove upto
    one item from the generator
    """

    return len(list(itertools.islice(items, 1))) == 1


def load_datasets(bbox, product, time):
    """
    Parse the query parameters and load and return the matching datasets. bbox is assumed to be
    [minimum longitude, minimum latitude, maximum longitude, maximum latitude]
    """

    query = dict()
    if product:
        query["product"] = product

    # Need to further parse time, we assume date as a range anf if time is present its a timestamp
    time_period = time.split("/")
    if len(time_period) == 2:
        query["time"] = Range(parse_time(time_period[0]), parse_time(time_period[1]))
    elif len(time_period) == 1:
        t = parse_time(time_period[0])
        if t.time() == datetime.time():
            query["time"] = Range(t, t + datetime.timedelta(days=1))
        else:
            query["time"] = t
    else:
        return []

    # bbox is in GeoJSON CRS (WGS84)
    query["lon"] = Range(bbox[0], bbox[2])
    query["lat"] = Range(bbox[1], bbox[3])

    return _model.STORE.index.datasets.search(**query)


def stac_datasets_validated(datasets):
    """
    Generates an extent validated stream of stac items
    """

    for dataset in datasets:
        stac_item = stac_dataset(dataset)
        if stac_item:
            yield stac_item


def stac_dataset(dataset):
    """
    Returns a dict corresponding to a stac item
    """

    # Parse extent first return None if fails
    shape, is_valid_extent = _utils.dataset_shape(dataset)
    if not (shape and is_valid_extent):
        _LOG.warning("Invalid extent or None extent in dataset %s", dataset.id)
        return None

    bbox = list(shape.bounds)
    metadata_doc = dataset.metadata_doc

    # Parse uri (and prefer remote uri s3)
    uris = dataset.uris
    remote_uris = [uri for uri in uris if urlparse(uri).scheme == "s3"]
    if remote_uris:
        uri = remote_uris[0]
    else:
        other_uris = [uri for uri in uris if urlparse(uri).scheme != "s3"]
        uri = other_uris[0] if other_uris else ""

    if metadata_doc["grid_spatial"]["projection"].get("valid_data", None):
        geodata = valid_coord_to_geojson(
            metadata_doc["grid_spatial"]["projection"]["valid_data"], dataset.crs
        )
    else:
        # Compute geometry from geo_ref_points
        points = [
            [
                list(point.values())
                for point in metadata_doc["grid_spatial"]["projection"][
                    "geo_ref_points"
                ].values()
            ]
        ]

        # last point and first point should be same
        points[0].append(points[0][0])

        geodata = valid_coord_to_geojson(
            {"type": "Polygon", "coordinates": points}, dataset.crs
        )

    # Convert the date to add time zone.
    center_dt = dataset.center_time
    center_dt = center_dt.replace(microsecond=0)
    time_zone = center_dt.tzinfo
    if not time_zone:
        center_dt = center_dt.replace(tzinfo=datetime.timezone.utc).isoformat()
    else:
        center_dt = center_dt.isoformat()

    # parent? We will have an empty parent for now
    stac_item = OrderedDict(
        [
            ("id", metadata_doc["id"]),
            ("type", "Feature"),
            ("bbox", bbox),
            ("geometry", geodata),
            (
                "properties",
                {"datetime": center_dt, "product_type": metadata_doc["product_type"]},
            ),
            ("links", [{"href": uri, "rel": "self"}]),
            (
                "assets",
                {
                    band_name: {
                        # "type"? "GeoTIFF" or image/vnd.stac.geotiff; cloud-optimized=true
                        "href": band_data["path"]
                    }
                    for band_name, band_data in metadata_doc["image"]["bands"].items()
                },
            ),
        ]
    )

    cfg = flask.current_app.config.get("STAC_SETTINGS")
    if cfg:
        if cfg.get("contact") and cfg["contact"].get("name"):
            stac_item["properties"]["provider"] = cfg["contact"]["name"]
        if cfg.get("license") and cfg["license"].get("name"):
            stac_item["properties"]["license"] = cfg["license"]["name"]
        if cfg.get("license") and cfg["license"].get("copyright"):
            stac_item["properties"]["copyright"] = cfg["license"]["copyright"]

    return stac_item


def valid_coord_to_geojson(valid_coord, crs):
    """
        The polygon coordinates come in Albers' format, which must be converted to
        lat/lon as in universal format in EPSG:4326
    """

    geo = Geometry(valid_coord, crs)
    return geo.to_crs(CRS("epsg:4326")).__geo_interface__


def _compute_bbox(stac_items):
    """
    Given extent validated stac items, compute the bounding box
    """

    if stac_items:
        return reduce(
            lambda x, y: dict(
                bbox=[
                    min(x["bbox"][0], y["bbox"][0]),
                    min(x["bbox"][1], y["bbox"][1]),
                    max(x["bbox"][2], y["bbox"][2]),
                    max(x["bbox"][3], y["bbox"][3]),
                ]
            ),
            stac_items,
        )["bbox"]
    return None
