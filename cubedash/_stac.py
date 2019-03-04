import itertools
import json
import logging
from collections import OrderedDict
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from functools import reduce
from typing import Iterable, Tuple
from urllib.parse import urlparse

import flask
from flask import Blueprint, abort, request, url_for

from datacube.model import Dataset, Range
from datacube.utils import parse_time
from datacube.utils.geometry import CRS, Geometry

from . import _model, _utils

_LOG = logging.getLogger(__name__)
bp = Blueprint("stac", __name__, url_prefix="/stac")

DATASET_LIMIT = 100
DEFAULT_PAGE_SIZE = 20


@bp.route("/")
def root():
    return abort(404, "Only /stac/search is currently supported")


@bp.route("/search", methods=["GET", "POST"])
def stac_search():
    if request.method == "GET":
        bbox = request.args.get("bbox")
        bbox = json.loads(bbox)
        time_ = request.args.get("time")
        product = request.args.get("product")
        limit = request.args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
        offset = request.args.get("offset", default=0, type=int)
    else:
        req_data = request.get_json()
        bbox = req_data.get("bbox")
        time_ = req_data.get("time")
        product = req_data.get("product")
        limit = req_data.get("limit") or DEFAULT_PAGE_SIZE
        offset = req_data.get("offset") or 0

    # bbox and time are compulsory
    if not bbox:
        abort(400, "bbox must be specified")
    if not time_:
        abort(400, "time must be specified")

    if offset >= DATASET_LIMIT:
        abort(400, "Server paging limit reached (first {} only)".format(DATASET_LIMIT))
    # If the request goes past MAX_DATASETS, shrink the limit to match it.
    if (offset + limit) > DATASET_LIMIT:
        limit = DATASET_LIMIT - offset
        # TODO: mention in the reply that we've hit a limit?

    if len(bbox) != 4:
        abort(400, "Expected bbox of size 4. [min lon, min lat, max long, max lat]")

    time_ = _parse_time_range(time_)

    return _utils.as_json(
        search_datasets_stac(
            product=product, bbox=bbox, time=time_, limit=limit, offset=offset
        )
    )


def search_datasets_stac(
    product: str,
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

    stac_items_all = stac_datasets_validated(load_datasets(bbox, product, time))

    stac_items_selected = list(itertools.islice(stac_items_all, offset, end_offset))

    result = dict(type="FeatureCollection", features=stac_items_selected)
    res_bbox = _compute_bbox(stac_items_selected)
    if res_bbox:
        result["bbox"] = res_bbox

    # Check whether stac_datasets has more datasets and we don't want to process
    # more than DATASET_LIMIT
    if _generator_not_empty(stac_items_all) and end_offset < DATASET_LIMIT:
        url_next = url_for(
            ".stac_search",
            product=product,
            bbox="[{},{},{},{}]".format(*bbox),
            time=_unparse_time_range(time),
            limit=limit,
            offset=end_offset,
        )
        result["links"] = [{"href": url_next, "rel": "next"}]
    return result


def _generator_not_empty(items):
    """
    Check whether the given generator is not empty. Note: this function can remove upto
    one item from the generator
    """

    return len(list(itertools.islice(items, 1))) == 1


def load_datasets(
    bbox: Tuple[float, float, float, float],
    product: str,
    time: Tuple[datetime, datetime],
) -> Iterable[Dataset]:
    """
    Parse the query parameters and load and return the matching datasets. bbox is assumed to be
    [minimum longitude, minimum latitude, maximum longitude, maximum latitude]
    """

    query = dict()
    if product:
        query["product"] = product

    query["time"] = Range(*time)

    # bbox is in GeoJSON CRS (WGS84)
    query["lon"] = Range(bbox[0], bbox[2])
    query["lat"] = Range(bbox[1], bbox[3])

    return _model.STORE.index.datasets.search(**query)


def _parse_time_range(time: str) -> Tuple[datetime, datetime]:
    """
     >>> _parse_time_range('1986-04-16T01:12:16/2097-05-10T00:24:21')
     (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(2097, 5, 10, 0, 24, 21))
     >>> _parse_time_range('1986-04-16T01:12:16')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(1986, 4, 16, 1, 12, 17))
    >>> _parse_time_range('1986-04-16')
    (datetime.datetime(1986, 4, 16, 0, 0, 0), datetime.datetime(1986, 4, 17, 0, 0, 0))
    """
    time_period = time.split("/")
    if len(time_period) == 2:
        return parse_time(time_period[0]), parse_time(time_period[1])
    elif len(time_period) == 1:
        t: datetime = parse_time(time_period[0])
        if t.time() == dt_time():
            return t, t + timedelta(days=1)
        else:
            return t, t + timedelta(minutes=1)


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

    center_dt = _utils.default_utc(dataset.center_time.replace(microsecond=0))

    # parent? We will have an empty parent for now
    stac_item = OrderedDict(
        [
            ("id", metadata_doc["id"]),
            ("type", "Feature"),
            ("bbox", bbox),
            ("geometry", geodata),
            (
                "properties",
                {
                    "datetime": center_dt.isoformat(),
                    "product_type": metadata_doc["product_type"],
                },
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
