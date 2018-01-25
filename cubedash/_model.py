from __future__ import absolute_import

import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple, Optional, Iterable

import fiona
import flask
import shapely
import shapely.geometry
import shapely.ops
import structlog
from flask import jsonify
from flask_caching import Cache

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Range, DatasetType
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS

from . import _utils as utils

from shapely.geometry import mapping, shape
import dateutil.parser

# Only do expensive queries "once a day"
# Enough time to last the remainder of the work day, but not enough to still be there the next morning
NAME = 'cubedash'
CACHE_LONG_TIMEOUT_SECS = 60 * 60 * 18
# TODO: Sensible cache directory handling?
CACHE_DIR = Path(__file__).parent.parent / 'web-cache'

SUMMARIES_DIR = Path(__file__).parent.parent / 'product-summaries'

app = flask.Flask(NAME)
cache = Cache(app=app,
              config=dict(
                  CACHE_KEY_PREFIX=NAME + '_cache_',
                  CACHE_TYPE='filesystem',
                  CACHE_DEFAULT_TIMEOUT=CACHE_LONG_TIMEOUT_SECS,
                  CACHE_THRESHOLD=8000,
                  CACHE_DIR=str(CACHE_DIR),
              ))


def as_json(o):
    return jsonify(jsonify_document(o))


# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking (hence validate=False).
index: Index = index_connect(application_name=NAME, validate_connection=False)

_LOG = structlog.get_logger()


class TimePeriodOverview(NamedTuple):
    # product_name: str
    # year: int
    # month: int

    dataset_count: int

    dataset_counts: Counter

    period: str

    footprint_geometry: shapely.geometry.base.BaseGeometry

    footprint_count: int

    @staticmethod
    def add_periods(periods: Iterable['TimePeriodOverview'], group_by_month=False):
        periods = list(periods)
        counter = Counter()
        period = None

        for p in periods:
            counter.update(p.dataset_counts)
            period = p.period

        if group_by_month:
            counter = Counter(
                datetime(date.year, date.month, 1).date()
                for date in counter.elements()
            )
            period = 'month'

        return TimePeriodOverview(
            sum(p.dataset_count for p in periods),
            counter,
            period,
            shapely.ops.unary_union([p.footprint_geometry for p in periods if p.footprint_geometry]),
            sum(p.footprint_count for p in periods),
        )


def _calculate_summary(product_name: str, time: Range, period: str) -> Optional[TimePeriodOverview]:
    datasets = index.datasets.search_eager(product=product_name, time=time)

    dataset_shapes = [shapely.geometry.asShape(ds.extent.to_crs(CRS('EPSG:4326')))
                      for ds in datasets if ds.extent]
    footprint_geometry = shapely.ops.unary_union(dataset_shapes) if dataset_shapes else None

    return TimePeriodOverview(len(datasets),
                              # TODO: AEST days rather than UTC is probably more useful for grouping AUS data.
                              Counter((d.time.begin.date() for d in datasets)),
                              period,
                              footprint_geometry,
                              len(dataset_shapes))


def write_product_summary(product: DatasetType, path: Path) -> TimePeriodOverview:
    # Update all months

    summaries = []
    for year in range(1985, datetime.today().year + 1):
        year_folder = path / ('%04d' % year)

        # if not year_folder.exists():
        write_year_summary(product, year, year_folder)

        summaries.append(read_summary(year_folder))

    summary = TimePeriodOverview.add_periods(summaries, group_by_month=True)
    summary_to_file(f'{product.name}', path, summary)
    return summary


def write_year_summary(product: DatasetType, year: int, path: Path) -> TimePeriodOverview:
    # Update all months

    summaries = []
    for month in range(1, 13):
        month_folder = path / ('%02d' % month)

        if not month_folder.exists():
            write_month_summary(product, year, month, month_folder)

        summaries.append(read_summary(month_folder))

    summary = TimePeriodOverview.add_periods(summaries)
    summary_to_file(f'{product.name}-{year}', path, summary)
    return summary


def write_month_summary(product: DatasetType, year: int, month: int, path: Path) -> TimePeriodOverview:
    # TODO: use temporary dir until done
    path.mkdir(parents=True)

    summary = _calculate_summary(product.name, utils.as_time_range(year, month), 'day')
    name = f'{product.name}-{year}-{month}'

    summary_to_file(name, path, summary)

    return summary


def read_summary(path: Path) -> TimePeriodOverview:
    with (path / 'timeline.json').open('r') as f:
        timeline = json.load(f)

    coverage_path = path / 'dataset-coverage.shp'

    with fiona.open(str(coverage_path)) as f:
        shapes = list(f)

    if not shapes:
        footprint = None
    else:
        if len(shapes) != 1:
            raise ValueError(f'Unexpected number of shapes in coverage? {len(shapes)}')

        footprint = shape(shapes[0]['geometry'])

    return TimePeriodOverview(
        timeline['total_count'],
        dataset_counts=Counter({dateutil.parser.parse(d): v for d, v in timeline['series'].items()}),
        period=timeline['period'],
        footprint_geometry=footprint,
        footprint_count=timeline['footprint_count']
    )


def summary_to_file(name: str, path: Path, summary: TimePeriodOverview):
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'str'},
    }
    with (path / 'timeline.json').open('w') as f:
        json.dump(
            dict(
                total_count=summary.dataset_count,
                footprint_count=summary.footprint_count,
                period=summary.period,
                series={d.isoformat(): v for d, v in summary.dataset_counts.items()}
            ),
            f
        )
    with fiona.open(str(path / 'dataset-coverage.shp'), 'w', 'ESRI Shapefile', schema) as f:
        if summary.footprint_geometry:
            f.write({
                'geometry': mapping(summary.footprint_geometry),
                'properties': {'id': name}
            })


def get_summary(
        product_name: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None) -> Optional[TimePeriodOverview]:
    # Days are small enough to calculate on the fly
    if year and month and day:
        return _calculate_summary(product_name, utils.as_time_range(year, month, day), 'day')

    # Otherwise load from file
    path = get_summary_path(product_name, year, month)
    if not path.exists():
        _LOG.warning('report.missing', product_name=product_name, year=year, month=month, day=day)
        return None
    return read_summary(path)


def get_summary_path(product_name: Optional[str] = None,
                     year: Optional[int] = None,
                     month: Optional[int] = None):
    path = SUMMARIES_DIR
    if product_name:
        path = path / product_name
    if year:
        path = path / ('%04d' % year)
    if month:
        path = path / ('%02d' % month)
    return path


def list_products():
    everything = index.datasets.types.get_all()
    return sorted(
        (
            (product, get_summary(product.name))
            for product in everything
            if get_summary_path(product.name).exists()
        ),
        key=lambda p: p[0].name
    )


def get_day(product_name: str, year: int, month: int, day: int):
    start = datetime(year, month, day)
    time_range = Range(start, start + timedelta(days=1))
    return index.datasets.search(product=product_name, time=time_range)
