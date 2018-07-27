from __future__ import absolute_import

from collections import Counter
from datetime import datetime
from typing import Iterable, Dict, Set
from typing import Optional, Tuple

import dataclasses
import shapely
import shapely.geometry
import shapely.ops
import structlog
from dataclasses import dataclass
from shapely.geometry.base import BaseGeometry

from cubedash import _utils
from datacube import utils as dc_utils
from datacube.model import Dataset
from datacube.model import Range

_LOG = structlog.get_logger()


# An acceptable use of x/y names.
# pylint: disable=invalid-name
@dataclass(frozen=True, order=True)
class GridCell(object):
    x: float
    y: float


@dataclass
class TimePeriodOverview:
    dataset_count: int

    timeline_dataset_counts: Counter
    grid_dataset_counts: Counter

    # GeoJSON FeatureCollection dict. But only when there's a small number of them.
    datasets_geojson: Optional[Dict]

    timeline_period: str

    time_range: Range

    footprint_geometry: shapely.geometry.MultiPolygon

    footprint_count: int

    # The most newly created dataset
    newest_dataset_creation_time: datetime

    # List of CRSes that these datasets are in
    crses: Set[str]

    # When this summary was generated
    summary_gen_time: datetime = dataclasses.field(default_factory=_utils.now_utc)

    @classmethod
    def add_periods(cls, periods: Iterable['TimePeriodOverview'], max_individual_datasets=800):
        periods = [p for p in periods if p is not None and p.dataset_count > 0]
        period = None

        if not periods:
            return TimePeriodOverview.empty()

        timeline_counter = Counter()
        for p in periods:
            timeline_counter.update(p.timeline_dataset_counts)
            period = p.timeline_period
        timeline_counter, period = cls._group_counter_if_needed(timeline_counter, period)

        grid_counter = Counter()
        for p in periods:
            grid_counter.update(p.grid_dataset_counts)

        with_valid_geometries = [p for p in periods
                                 if p.footprint_count and p.footprint_geometry
                                 and p.footprint_geometry.is_valid
                                 and not p.footprint_geometry.is_empty]

        try:
            geometry_union = shapely.ops.unary_union(
                [p.footprint_geometry for p in with_valid_geometries]
            ) if with_valid_geometries else None
        except ValueError:
            _LOG.warn(
                'summary.footprint.union', exc_info=True
            )
            # Attempt 2 at union: Exaggerate the overlap *slightly* to
            # avoid non-noded intersection.
            # TODO: does shapely have a snap-to-grid?
            geometry_union = shapely.ops.unary_union(
                [p.footprint_geometry.buffer(0.001) for p in with_valid_geometries]
            ) if with_valid_geometries else None

        total_datasets = sum(p.dataset_count for p in periods)
        all_datasets_geojson = cls._combined_geojson(periods) if total_datasets < max_individual_datasets else None

        return TimePeriodOverview(
            dataset_count=total_datasets,
            timeline_dataset_counts=timeline_counter,
            timeline_period=period,
            grid_dataset_counts=grid_counter,
            datasets_geojson=all_datasets_geojson,
            time_range=Range(
                min(r.time_range.begin for r in periods),
                max(r.time_range.end for r in periods)
            ),
            footprint_geometry=geometry_union,
            footprint_count=sum(p.footprint_count for p in with_valid_geometries),
            newest_dataset_creation_time=max(
                (
                    p.newest_dataset_creation_time
                    for p in periods if p.newest_dataset_creation_time is not None
                ),
                default=None
            ),
            crses=set.union(*(o.crses for o in periods)),
            summary_gen_time=min(
                (
                    p.summary_gen_time
                    for p in periods if p.summary_gen_time is not None
                ),
                default=None
            ),
        )

    @classmethod
    def _combined_geojson(cls, periods):
        all_datasets_geojson = dict(
            type='FeatureCollection',
            features=[],
        )
        for p in periods:
            if p.datasets_geojson is not None:
                all_datasets_geojson['features'].extend(p.datasets_geojson['features'])
        return all_datasets_geojson

    @staticmethod
    def empty():
        return TimePeriodOverview(0, None, None, None, None, None, None, None, None, None)

    @staticmethod
    def _group_counter_if_needed(counter, period):
        if len(counter) > 365:
            if period == 'day':
                counter = Counter(
                    datetime(date.year, date.month, 1).date()
                    for date in counter.elements()
                )
                period = 'month'
            elif period == 'month':
                counter = Counter(
                    datetime(date.year, 1, 1).date()
                    for date in counter.elements()
                )
                period = 'year'

        return counter, period



def _has_shape(datasets: Tuple[Dataset, Tuple[BaseGeometry, bool]]) -> bool:
    dataset, (shape, was_valid) = datasets
    return shape is not None


def _dataset_created(dataset: Dataset) -> Optional[datetime]:
    if 'created' in dataset.metadata.fields:
        return dataset.metadata.created

    value = dataset.metadata.creation_dt
    if value:
        try:
            return _utils.default_utc(dc_utils.parse_time(value))
        except ValueError:
            _LOG.warn('invalid_dataset.creation_dt', dataset_id=dataset.id, value=value)

    return None


def _datasets_to_feature(datasets: Iterable[Tuple[Dataset, Tuple[BaseGeometry, bool]]]):
    return {
        'type': 'FeatureCollection',
        'features': [_dataset_to_feature(ds_valid) for ds_valid in datasets]
    }


def _dataset_to_feature(ds: Tuple[Dataset, Tuple[BaseGeometry, bool]]):
    dataset, (shape, valid_extent) = ds
    return {
        'type': 'Feature',
        'geometry': shape.__geo_interface__,
        'properties': {
            'id': str(dataset.id),
            'label': _utils.dataset_label(dataset),
            'valid_extent': valid_extent,
            'start_time': dataset.time.begin.isoformat()
        }
    }
