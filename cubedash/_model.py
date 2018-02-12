from __future__ import absolute_import

import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple, Optional, Iterable, Tuple, Dict

import dateutil.parser
import fiona
import flask
import shapely
import shapely.geometry
import shapely.ops
import structlog
from flask import jsonify
from flask_caching import Cache
from shapely.geometry.base import BaseGeometry

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Range, DatasetType, Dataset
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS
from . import _utils as utils
import pytz

NAME = 'cubedash'
# Pre-computed summaries of products (to avoid doing them on page load).
SUMMARIES_DIR = Path(__file__).parent.parent / 'product-summaries'

app = flask.Flask(NAME)
cache = Cache(
    app=app,
    config={'CACHE_TYPE': 'simple'}
)

# Group datasets using this timezone when counting them.
# Aus data comes from Alice Springs
_GROUPING_TIME_ZONE = pytz.timezone('Australia/Darwin')


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

    # GeoJSON FeatureCollection dict. But only when there's a small number of them.
    datasets_geojson: Optional[Dict]

    period: str

    time_range: Range

    footprint_geometry: shapely.geometry.base.BaseGeometry

    footprint_count: int

    @staticmethod
    def add_periods(periods: Iterable['TimePeriodOverview'], group_by_month=False):
        periods = [p for p in periods if p.dataset_count > 0]
        counter = Counter()
        period = None

        if not periods:
            return TimePeriodOverview(0, None, None, None, None, None, None)

        for p in periods:
            counter.update(p.dataset_counts)
            period = p.period

        if group_by_month:
            counter = Counter(
                datetime(date.year, date.month, 1).date()
                for date in counter.elements()
            )
            period = 'month'

        with_valid_geometries = [p for p in periods
                                 if p.footprint_geometry
                                 and p.footprint_geometry.is_valid
                                 and not p.footprint_geometry.is_empty]

        return TimePeriodOverview(
            sum(p.dataset_count for p in periods),
            counter,
            None,
            period,
            Range(
                min(r.time_range.begin for r in periods),
                max(r.time_range.end for r in periods)
            ),
            shapely.ops.unary_union(
                [p.footprint_geometry for p in with_valid_geometries]
            ) if with_valid_geometries else None,
            sum(p.footprint_count for p in with_valid_geometries),
        )


def _dataset_shape(ds: Dataset):
    try:
        extent = ds.extent
    except AttributeError:
        # `ds.extent` throws an exception on telemetry datasets, as they have no grid_spatial. It probably shouldn't.
        return None

    if extent is None:
        return None

    return shapely.geometry.asShape(extent.to_crs(CRS('EPSG:4326')))


def _calculate_summary(product_name: str, time: Range) -> Optional[TimePeriodOverview]:
    log = _LOG.bind(product=product_name, time=time)
    log.debug("summary.calc")

    datasets = [
        (dataset, _dataset_shape(dataset))
        for dataset in index.datasets.search(product=product_name, time=time)
    ]
    dataset_shapes = [
        shape for dataset, shape in datasets
        if shape and shape.is_valid and not shape.is_empty
    ]
    footprint_geometry = shapely.ops.unary_union(dataset_shapes) if dataset_shapes else None

    summary = TimePeriodOverview(
        len(datasets),
        Counter((_GROUPING_TIME_ZONE.fromutc(dataset.time.begin).date() for dataset, shape in datasets)),
        datasets_to_feature(datasets) if 0 < len(dataset_shapes) < 250 else None,
        'day',
        time,
        footprint_geometry,
        len(dataset_shapes)
    )
    log.debug(
        "summary.calc.done",
        dataset_count=summary.dataset_count,
        footprints_missing=summary.dataset_count - summary.footprint_count
    )
    return summary


def datasets_to_feature(datasets: Iterable[Tuple[Dataset, BaseGeometry]]):
    return {
        'type': 'FeatureCollection',
        'features': [dataset_to_feature(ds) for ds in datasets if ds[1]]
    }


def dataset_to_feature(ds: Tuple[Dataset, BaseGeometry]):
    dataset, shape = ds
    return {
        'type': 'Feature',
        'geometry': shape.__geo_interface__,
        'properties': {
            'id': str(dataset.id),
            'label': utils.dataset_label(dataset),
            'start_time': dataset.time.begin.isoformat()
        }
    }


def generate_summary() -> TimePeriodOverview:
    """
    Write (or replace) the summary of all products that we've got data for.
    """
    products = list_product_summaries()
    summary = TimePeriodOverview.add_periods(
        summary for product, summary in products
    )

    summary_to_file('all', get_summary_path(), summary)
    return summary


def write_product_summary(product: DatasetType, path: Path) -> TimePeriodOverview:
    """
    Generate and write a summary of the given product
    """
    summaries = []
    for year in range(1985, datetime.today().year + 1):
        year_folder = path / ('%04d' % year)

        if year_folder.exists():
            s = read_summary(year_folder)
        else:
            s = _write_year_summary(product, year, year_folder)

        summaries.append(s)

    summary = TimePeriodOverview.add_periods(summaries, group_by_month=True)

    summary_to_file(f'{product.name}', path, summary)
    return summary


def _write_year_summary(product: DatasetType, year: int, path: Path) -> TimePeriodOverview:
    summaries = []
    for month in range(1, 13):
        month_folder = path / ('%02d' % month)

        if month_folder.exists():
            s = read_summary(month_folder)
        else:
            s = _write_month_summary(product, year, month, month_folder)

        summaries.append(s)

    summary = TimePeriodOverview.add_periods(summaries)
    if summary.dataset_count > 0:
        summary_to_file(f'{product.name}-{year}', path, summary)
    return summary


def _write_month_summary(product: DatasetType, year: int, month: int, path: Path) -> TimePeriodOverview:
    summary = _calculate_summary(product.name, utils.as_time_range(year, month))
    name = f'{product.name}-{year}-{month}'

    if summary.dataset_count > 0:
        path.mkdir(parents=True)
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

        footprint = shapely.geometry.shape(shapes[0]['geometry'])

    return TimePeriodOverview(
        timeline['total_count'],
        dataset_counts=Counter(
            {dateutil.parser.parse(d): v for d, v in timeline['series'].items()}
        ) if timeline.get('series') else None,
        datasets_geojson=timeline.get('datasets_geojson'),
        period=timeline['period'],
        time_range=Range(
            dateutil.parser.parse(timeline['time_range'][0]),
            dateutil.parser.parse(timeline['time_range'][1])
        ) if timeline.get('time_range') else None,
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
                datasets_geojson=summary.datasets_geojson,
                period=summary.period,
                time_range=[
                    summary.time_range[0].isoformat(),
                    summary.time_range[1].isoformat()
                ] if summary.time_range else None,
                series={
                    d.isoformat(): v for d, v in summary.dataset_counts.items()
                } if summary.dataset_counts else None,
            ),
            f
        )
    with fiona.open(str(path / 'dataset-coverage.shp'), 'w', 'ESRI Shapefile', schema) as f:
        if summary.footprint_geometry:
            f.write({
                'geometry': shapely.geometry.mapping(summary.footprint_geometry),
                'properties': {'id': name}
            })


@cache.memoize(timeout=60)
def get_summary(
        product_name: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None) -> Optional[TimePeriodOverview]:
    # Days are small enough to calculate on the fly
    if year and month and day:
        return _calculate_summary(product_name, utils.as_time_range(year, month, day))

    # Otherwise load from file
    path = get_summary_path(product_name, year, month)
    if not path.exists():
        _LOG.warning('report.missing', product_name=product_name, year=year, month=month, day=day, expected_path=path)
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


@cache.memoize(timeout=120)
def list_product_summaries() -> Iterable[Tuple[DatasetType, TimePeriodOverview]]:
    """
    The list of products that we have generated reports for.
    """
    everything = index.datasets.types.get_all()
    existing_products = sorted(
        (
            (product, get_summary(product.name))
            for product in everything
            if get_summary_path(product.name).exists()
        ),
        key=lambda p: p[0].name
    )
    if not existing_products:
        raise RuntimeError('No product reports. Run `python -m cubedash.generate --all` to generate some.')

    return existing_products


def get_day(product_name: str, year: int, month: int, day: int):
    start = datetime(year, month, day)
    time_range = Range(start, start + timedelta(days=1))
    return index.datasets.search(product=product_name, time=time_range)
