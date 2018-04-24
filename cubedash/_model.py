from __future__ import absolute_import

from datetime import datetime, timedelta
from pathlib import Path

import flask
import shapely.geometry
import structlog
from dateutil import tz
from flask import jsonify
from flask_caching import Cache

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Dataset, Range
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS

NAME = "cubedash"
# Pre-computed summaries of products (to avoid doing them on page load).
SUMMARIES_DIR = Path(__file__).parent.parent / "product-summaries"

app = flask.Flask(NAME)
cache = Cache(app=app, config={"CACHE_TYPE": "simple"})

# Group datasets using this timezone when counting them.
# Aus data comes from Alice Springs
GROUPING_TIME_ZONE = tz.gettz("Australia/Darwin")
# If there's fewer than this many datasets, display them as individual polygons in
# the browser. Too many can bog down the browser's performance.
# (Otherwise dataset footprint is shown as a single composite polygon)
MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY = 300


def as_json(o):
    return jsonify(jsonify_document(o))


# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking
# (hence validate=False).
index: Index = index_connect(application_name=NAME, validate_connection=False)

_LOG = structlog.get_logger()


def get_day(product_name: str, year: int, month: int, day: int):
    start = datetime(year, month, day)
    time_range = Range(start, start + timedelta(days=1))
    return index.datasets.search(product=product_name, time=time_range)


def dataset_shape(ds: Dataset):
    try:
        extent = ds.extent
    except AttributeError:
        # `ds.extent` throws an exception on telemetry datasets,
        # as they have no grid_spatial. It probably shouldn't.
        return None

    if extent is None:
        return None

    return shapely.geometry.asShape(extent.to_crs(CRS("EPSG:4326")))
