from __future__ import absolute_import

import dataclasses
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Optional, Set, Tuple, Union

import shapely
import shapely.geometry
import shapely.ops
import structlog
from shapely.geometry.base import BaseGeometry

from cubedash import _utils
from datacube import utils as dc_utils
from datacube.model import Dataset, Range

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

    timeline_period: str

    time_range: Range

    footprint_geometry: Union[shapely.geometry.MultiPolygon, shapely.geometry.Polygon]

    footprint_count: int

    # The most newly created dataset
    newest_dataset_creation_time: datetime

    # List of CRSes that these datasets are in
    crses: Set[str]

    size_bytes: int

    # When this summary was generated. Set on the server.
    summary_gen_time: datetime = None

    @classmethod
    def add_periods(cls, periods: Iterable["TimePeriodOverview"]):
        periods = [p for p in periods if p is not None and p.dataset_count > 0]
        period = "day"

        timeline_counter = Counter()
        for p in periods:
            timeline_counter.update(p.timeline_dataset_counts)
            period = p.timeline_period
        timeline_counter, period = cls._group_counter_if_needed(
            timeline_counter, period
        )

        grid_counter = Counter()
        for p in periods:
            grid_counter.update(p.grid_dataset_counts)

        with_valid_geometries = [
            p
            for p in periods
            if p.footprint_count
            and p.footprint_geometry
            and p.footprint_geometry.is_valid
            and not p.footprint_geometry.is_empty
        ]

        try:
            geometry_union = (
                shapely.ops.unary_union(
                    [p.footprint_geometry for p in with_valid_geometries]
                )
                if with_valid_geometries
                else None
            )
        except ValueError:
            _LOG.warn("summary.footprint.union", exc_info=True)
            # Attempt 2 at union: Exaggerate the overlap *slightly* to
            # avoid non-noded intersection.
            # TODO: does shapely have a snap-to-grid?
            geometry_union = (
                shapely.ops.unary_union(
                    [p.footprint_geometry.buffer(0.001) for p in with_valid_geometries]
                )
                if with_valid_geometries
                else None
            )

        total_datasets = sum(p.dataset_count for p in periods)

        return TimePeriodOverview(
            dataset_count=total_datasets,
            timeline_dataset_counts=timeline_counter,
            timeline_period=period,
            grid_dataset_counts=grid_counter,
            time_range=Range(
                min(r.time_range.begin for r in periods) if periods else None,
                max(r.time_range.end for r in periods) if periods else None,
            ),
            footprint_geometry=geometry_union,
            footprint_count=sum(p.footprint_count for p in with_valid_geometries),
            newest_dataset_creation_time=max(
                (
                    p.newest_dataset_creation_time
                    for p in periods
                    if p.newest_dataset_creation_time is not None
                ),
                default=None,
            ),
            crses=set.union(*(o.crses for o in periods)) if periods else set(),
            summary_gen_time=min(
                (p.summary_gen_time for p in periods if p.summary_gen_time is not None),
                default=None,
            ),
            size_bytes=sum(p.size_bytes for p in periods if p.size_bytes is not None),
        )

    @staticmethod
    def _group_counter_if_needed(counter, period):
        if len(counter) > 365:
            if period == "day":
                counter = Counter(
                    datetime(date.year, date.month, 1).date()
                    for date in counter.elements()
                )
                period = "month"
            elif period == "month":
                counter = Counter(
                    datetime(date.year, 1, 1).date() for date in counter.elements()
                )
                period = "year"

        return counter, period


def _has_shape(datasets: Tuple[Dataset, Tuple[BaseGeometry, bool]]) -> bool:
    dataset, (shape, was_valid) = datasets
    return shape is not None


def _dataset_created(dataset: Dataset) -> Optional[datetime]:
    if "created" in dataset.metadata.fields:
        return dataset.metadata.created

    value = dataset.metadata.creation_dt
    if value:
        try:
            return _utils.default_utc(dc_utils.parse_time(value))
        except ValueError:
            _LOG.warn("invalid_dataset.creation_dt", dataset_id=dataset.id, value=value)

    return None


def _datasets_to_feature(datasets: Iterable[Tuple[Dataset, Tuple[BaseGeometry, bool]]]):
    return {
        "type": "FeatureCollection",
        "features": [_dataset_to_feature(ds_valid) for ds_valid in datasets],
    }


def _dataset_to_feature(ds: Tuple[Dataset, Tuple[BaseGeometry, bool]]):
    dataset, (shape, valid_extent) = ds
    return {
        "type": "Feature",
        "geometry": shape.__geo_interface__,
        "properties": {
            "id": str(dataset.id),
            "label": _utils.dataset_label(dataset),
            "valid_extent": valid_extent,
            "start_time": dataset.time.begin.isoformat(),
        },
    }
