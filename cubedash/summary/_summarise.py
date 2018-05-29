from __future__ import absolute_import

from collections import Counter
from datetime import datetime
from typing import Dict, Iterable, NamedTuple, Optional, Tuple

import pandas as pd
import shapely
import shapely.geometry
import shapely.ops
import structlog
from dateutil import tz
from shapely.geometry.base import BaseGeometry

from cubedash import _utils
from datacube import utils as dc_utils
from datacube.index._api import Index
from datacube.model import Dataset, Range

_LOG = structlog.get_logger()


class TimePeriodOverview(NamedTuple):
    dataset_count: int

    dataset_counts: Counter

    # GeoJSON FeatureCollection dict. But only when there's a small number of them.
    datasets_geojson: Optional[Dict]

    period: str

    time_range: Range

    footprint_geometry: shapely.geometry.base.BaseGeometry

    footprint_count: int

    # The most newly created dataset
    newest_dataset_creation_time: datetime

    # When this summary was generated
    summary_gen_time: datetime

    @classmethod
    def add_periods(cls, periods: Iterable["TimePeriodOverview"]):
        periods = [p for p in periods if p.dataset_count > 0]
        counter = Counter()
        period = None

        if not periods:
            return TimePeriodOverview(0, None, None, None, None, None, None, None, None)

        for p in periods:
            counter.update(p.dataset_counts)
            period = p.period

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

        return TimePeriodOverview(
            sum(p.dataset_count for p in periods),
            counter,
            None,
            period,
            Range(
                min(r.time_range.begin for r in periods),
                max(r.time_range.end for r in periods),
            ),
            geometry_union,
            sum(p.footprint_count for p in with_valid_geometries),
            max(
                (
                    p.newest_dataset_creation_time
                    for p in periods
                    if p.newest_dataset_creation_time is not None
                ),
                default=None,
            ),
            min(
                (p.summary_gen_time for p in periods if p.summary_gen_time is not None),
                default=None,
            ),
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


class SummaryStore:
    def __init__(self, index: Index, log=_LOG) -> None:
        self._index = index
        self._log = log
        self._update_listeners = []

    # Group datasets using this timezone when counting them.
    # Aus data comes from Alice Springs
    GROUPING_TIME_ZONE = tz.gettz("Australia/Darwin")
    # If there's fewer than this many datasets, display them as individual polygons in
    # the browser. Too many can bog down the browser's performance.
    # (Otherwise dataset footprint is shown as a single composite polygon)
    MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY = 600

    def init(self) -> bool:
        """
        Create the store if it doesn't already exist

        Returns true if was created, false if already existed.
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

        self.put(product_name, year, month, day, summary)
        for listener in self._update_listeners:
            listener(product_name, year, month, day, summary)

        return summary

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

        Default implementation uses the pure index api.
        """
        log = self._log.bind(product_name=product_name, time=time)
        log.debug("summary.query")

        datasets = [
            (dataset, _utils.dataset_shape(dataset))
            for dataset in self._index.datasets.search(product=product_name, time=time)
        ]
        log.debug("summary.query.done")

        log.debug("summary.calc")
        dataset_shapes = list(filter(_has_shape, datasets))

        footprint_geometry = (
            shapely.ops.unary_union([shape for _, (shape, _) in dataset_shapes])
            if dataset_shapes
            else None
        )

        # Initialise all requested days as zero
        day_counts = Counter(
            {d.date(): 0 for d in pd.date_range(time.begin, time.end, closed="left")}
        )
        day_counts.update(
            _utils.default_utc(dataset.center_time)
            .astimezone(self.GROUPING_TIME_ZONE)
            .date()
            for dataset, shape in datasets
        )

        summary = TimePeriodOverview(
            len(datasets),
            day_counts,
            _datasets_to_feature(dataset_shapes)
            if 0 < len(dataset_shapes) < self.MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY
            else None,
            "day",
            time,
            footprint_geometry,
            len(dataset_shapes),
            max(
                (_dataset_created(dataset) for dataset, shape in datasets), default=None
            ),
            _utils.default_utc(datetime.utcnow()),
        )
        log.debug(
            "summary.calc.done",
            dataset_count=summary.dataset_count,
            footprints_missing=summary.dataset_count - summary.footprint_count,
        )
        return summary


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
