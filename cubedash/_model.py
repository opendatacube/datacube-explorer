from __future__ import absolute_import
from __future__ import absolute_import

from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple, Optional, Iterable

import flask
import shapely
import shapely.geometry
import shapely.ops
from flask import jsonify
from flask_caching import Cache

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Range
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS

# Only do expensive queries "once a day"
# Enough time to last the remainder of the work day, but not enough to still be there the next morning
NAME = 'cubedash'
CACHE_LONG_TIMEOUT_SECS = 60 * 60 * 18
# TODO: Sensible cache directory handling?
CACHE_DIR = Path(__file__).parent.parent / 'web-cache'

app = flask.Flask(NAME)
cache = Cache(app=app,
              config=dict(
                  CACHE_KEY_PREFIX=NAME + '_cache_',
                  CACHE_TYPE='filesystem',
                  CACHE_DEFAULT_TIMEOUT=CACHE_LONG_TIMEOUT_SECS,
                  CACHE_THRESHOLD=2000,
                  CACHE_DIR=str(CACHE_DIR),
              ))


def as_json(o):
    return jsonify(jsonify_document(o))


# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking (hence validate=False).
index: Index = index_connect(application_name=NAME, validate_connection=False)


class TimePeriodOverview(NamedTuple):
    # product_name: str
    # year: int
    # month: int

    dataset_count: int

    dataset_counts: Counter

    footprint_geometry: shapely.geometry.base.BaseGeometry

    footprint_count: int


def _add_periods(periods: Iterable['TimePeriodOverview'], group_by_month=False):
    counter = sum(p.dataset_counts for p in periods)

    if group_by_month:
        counter = Counter(
            (date.year, date.month)
            for date in counter.elements()
        )

    return TimePeriodOverview(
        sum(p.dataset_count for p in periods),
        counter,
        shapely.ops.unary_union(p.footprint_geometry for p in periods),
        sum(p.footprint_count for p in periods),
    )


def next_month(date: datetime):
    if date.month == 12:
        return datetime(date.year + 1, 1, 1)

    return datetime(date.year, date.month + 1, 1)


def _get_month_summary(product_name: str, year: int, month: int) -> Optional[TimePeriodOverview]:
    start = datetime(year, month, 1)
    time = Range(start, next_month(start))

    datasets = index.datasets.search_eager(product=product_name, time=time)

    if not datasets:
        return None

    dataset_shapes = [shapely.geometry.asShape(ds.extent.to_crs(CRS('EPSG:4326')))
                      for ds in datasets if ds.extent]
    footprint_geometry = shapely.ops.unary_union(dataset_shapes)

    return TimePeriodOverview(len(datasets),
                              # TODO: AEST days rather than UTC is probably more useful for grouping AUS data.
                              Counter((d.time.begin.date() for d in datasets)),
                              footprint_geometry,
                              len(dataset_shapes))


@cache.memoize()
def get_summary(product_name, year, month):
    # type: (str, Optional[int], Optional[int]) -> TimePeriodOverview

    if year and month:
        # Specific month
        return _get_month_summary(product_name, year, month)
    elif year:
        # All months
        return _add_periods(
            get_summary(product_name, year, month)
            for month in range(1, 13)
        )
    else:
        # All years
        return _add_periods(
            (
                get_summary(product_name, year, None)
                for year in (range(1985, datetime.today().year + 1))
            ),
            group_by_month=True
        )


def get_day(product_name: str, year: int, month: int, day: int):
    start = datetime(year, month, day)
    time_range = Range(start, start + timedelta(days=1))
    return index.datasets.search(product=product_name, time=time_range)
