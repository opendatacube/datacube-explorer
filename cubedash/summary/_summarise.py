from __future__ import absolute_import

import dataclasses
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import shapely
import shapely.geometry
import shapely.ops
import structlog
from dateutil import tz
from shapely.geometry.base import BaseGeometry

from cubedash import _utils
from datacube import utils as dc_utils
from datacube.index import Index
from datacube.model import Dataset, Range

_LOG = structlog.get_logger()


@dataclass(frozen=True)
class GridCell(object):
    x: float
    y: float


@dataclass
class TimePeriodOverview:
    dataset_count: int

    timeline_dataset_counts: Counter

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
    def add_periods(
        cls, periods: Iterable["TimePeriodOverview"], max_individual_datasets=800
    ):
        periods = [p for p in periods if p.dataset_count > 0]
        counter = Counter()
        period = None

        if not periods:
            return TimePeriodOverview.empty()

        for p in periods:
            counter.update(p.timeline_dataset_counts)
            period = p.timeline_period

        counter, period = cls._group_counter_if_needed(counter, period)

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
        all_datasets_geojson = (
            cls._combined_geojson(periods)
            if total_datasets < max_individual_datasets
            else None
        )

        return TimePeriodOverview(
            dataset_count=total_datasets,
            timeline_dataset_counts=counter,
            timeline_period=period,
            datasets_geojson=all_datasets_geojson,
            time_range=Range(
                min(r.time_range.begin for r in periods),
                max(r.time_range.end for r in periods),
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
            crses=set.union(*(o.crses for o in periods)),
            summary_gen_time=min(
                (p.summary_gen_time for p in periods if p.summary_gen_time is not None),
                default=None,
            ),
        )

    @classmethod
    def _combined_geojson(cls, periods):
        all_datasets_geojson = dict(type="FeatureCollection", geometries=[])
        for p in periods:
            if p.datasets_geojson is not None:
                all_datasets_geojson["geometries"].extend(
                    p.datasets_geojson["geometries"]
                )
        return all_datasets_geojson

    @staticmethod
    def empty():
        return TimePeriodOverview(0, None, None, None, None, None, None, None, None)

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


class SummaryStore:
    def __init__(self, index: Index, log=_LOG) -> None:
        self._index = index
        self._log = log
        self._update_listeners = []

    # Group datasets using this timezone when counting them.
    # Aus data comes from Alice Springs
    GROUPING_TIME_ZONE_NAME = "Australia/Darwin"
    # cache
    GROUPING_TIME_ZONE_TZ = tz.gettz(GROUPING_TIME_ZONE_NAME)

    # If there's fewer than this many datasets, display them as individual polygons in
    # the browser. Too many can bog down the browser's performance.
    # (Otherwise dataset footprint is shown as a single composite polygon)
    MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY = 600

    def init(self):
        """
        Create the store if it doesn't already exist
        """
        # Default: nothing to set up.
        pass

    def get(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
    ) -> Optional[TimePeriodOverview]:
        """Get a cached summary if one exists. Should always return quickly."""
        raise NotImplementedError("Get summary")

    def put(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
        summary: TimePeriodOverview,
    ):
        """Put a summary in the cache, overridding any existing"""
        raise NotImplementedError("Write summary")

    def has(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
    ) -> bool:
        return self.get(product_name, year, month, day) is not None

    def get_or_update(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
    ):
        """
        Get a cached summary if exists, otherwise generate one

        Note that generating one can be *extremely* slow.
        """
        summary = self.get(product_name, year, month, day)
        if summary:
            return summary
        else:
            summary = self.update(product_name, year, month, day)
            return summary

    def update(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
        generate_missing_children=True,
    ):
        """Update the given summary and return the new one"""

        if year and month and day:
            # Don't store days, they're quick.
            return self.calculate_summary(
                product_name, _utils.as_time_range(year, month, day)
            )
        elif year and month:
            summary = self.calculate_summary(
                product_name, _utils.as_time_range(year, month)
            )
        elif year:
            summary = TimePeriodOverview.add_periods(
                self.get_or_update(product_name, year, month_, None)
                for month_ in range(1, 13)
            )
        elif product_name:
            summary = TimePeriodOverview.add_periods(
                self.get_or_update(product_name, year_, None, None)
                for year_ in range(1985, datetime.today().year + 1)
            )
        else:
            summary = TimePeriodOverview.add_periods(
                self.get_or_update(product.name, None, None, None)
                for product in self._index.products.get_all()
            )

        self._do_put(product_name, year, month, day, summary)

        for listener in self._update_listeners:
            listener(product_name, year, month, day, summary)
        return summary

    def _do_put(self, product_name, year, month, day, summary):

        # Don't bother storing empty periods that are outside of the existing range.
        # This doesn't have to be exact (note that someone may update in parallel too).
        if summary.dataset_count == 0 and (year or month):
            product_extent = self.get(product_name, None, None, None)
            if (not product_extent) or (not product_extent.time_range):
                return

            start, end = product_extent.time_range
            if datetime(year, month or 1, day or 1) < start:
                return
            if datetime(year, month or 12, day or 28) > end:
                return

        self.put(product_name, year, month, day, summary)

    def list_complete_products(self) -> Iterable[str]:
        """
        List products with summaries available.
        """
        all_products = self._index.datasets.types.get_all()
        existing_products = sorted(
            (
                product.name
                for product in all_products
                if self.has(product.name, None, None, None)
            )
        )
        return existing_products

    def get_last_updated(self) -> Optional[datetime]:
        """Time of last update, if known"""
        return None

    def calculate_summary(self, product_name: str, time: Range) -> TimePeriodOverview:
        """
        Create a summary of the given product/time range.
        """
        raise NotImplementedError("Summary calc")


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
