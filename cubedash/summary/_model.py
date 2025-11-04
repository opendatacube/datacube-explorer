from __future__ import annotations

import warnings
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import shapely
import shapely.ops
import structlog
from datacube.model import Range
from odc.geo.geom import Geometry
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry
from typing_extensions import override

_LOG = structlog.stdlib.get_logger()


@dataclass
class TimePeriodOverview:
    # These four elements make up a pseudo-id of the time period we've summarised.
    #
    # -> None means "all"
    product_name: str
    year: int | None
    month: int | None
    day: int | None

    dataset_count: int
    timeline_dataset_counts: Counter
    region_dataset_counts: Counter

    timeline_period: str

    time_range: Range | None

    footprint_geometry: shapely.geometry.MultiPolygon | shapely.geometry.Polygon | None
    footprint_crs: str | None

    footprint_count: int

    # The most newly created dataset
    newest_dataset_creation_time: datetime | None

    # List of CRSes that these datasets are in
    crses: set[str]

    size_bytes: int

    # What version of our product table this was based on (the last_refresh_time on ProductSummary)
    product_refresh_time: datetime

    # When this summary was generated. Set on the server.
    summary_gen_time: datetime | None = None

    @override
    def __str__(self) -> str:
        return (
            f"{self.label} "
            f"({self.dataset_count} dataset{'s' if self.dataset_count > 1 else ''})"
        )

    @property
    def label(self) -> str:
        return " ".join([(str(p) if p else "all") for p in self.period_tuple])

    @property
    def period_tuple(self) -> tuple[str, int | None, int | None, int | None]:
        """
        This is the pseudo-id of the product time period we've summarised.

        Any of them can be None to represent 'all'
        """
        return self.product_name, self.year, self.month, self.day

    @period_tuple.setter
    def period_tuple(self, v: tuple[str, int | None, int | None, int | None]) -> None:
        self.product_name, self.year, self.month, self.day = v

    def as_flat_period(self) -> tuple[str, date]:
        """
        How we "flatten" the time-slice for storage in DB columns. Must remain stable!

        A "period type" enum, and a single date.
        """
        return self.flat_period_representation(self.year, self.month, self.day)

    @classmethod
    def flat_period_representation(
        cls, year: int | None, month: int | None, day: int | None
    ) -> tuple[str, date]:
        period = "all"
        if year:
            period = "year"
        if month:
            period = "month"
        if day:
            period = "day"

        return period, date(year or 1900, month or 1, day or 1)

    @classmethod
    def from_flat_period_representation(
        cls, period_type: str, start_day: date
    ) -> tuple[int | None, int | None, int | None]:
        year = None
        month = None
        day = None
        if period_type != "all":
            year = start_day.year
            if period_type != "year":
                month = start_day.month
                if period_type != "month":
                    day = start_day.day
        return year, month, day

    @classmethod
    def empty(
        cls, product_name: str, product_refresh_time: datetime
    ) -> TimePeriodOverview:
        return cls.add_periods(product_name, product_refresh_time, [])

    @classmethod
    def add_periods(
        cls,
        product_name: str,
        product_refresh_time: datetime,
        periods: Iterable[TimePeriodOverview | None],
        # This is in CRS units. Albers, so 1KM.
        # Lower value will have a more accurate footprint and much larger page load times.
        footprint_tolerance: float = 1000.0,
    ) -> TimePeriodOverview:
        periods = [p for p in periods if p is not None and p.dataset_count > 0]
        period = "day"
        crses = {p.footprint_crs for p in periods}
        if not crses:
            footprint_crs = None
        elif len(crses) == 1:
            [footprint_crs] = crses
        else:
            # All generated summaries should be the same, so this can only occur if someone's changes
            # output crs setting on an existing cubedash instance.
            raise NotImplementedError("Time summaries use inconsistent CRSes.")

        timeline_counter: Counter = Counter()
        for p in periods:
            timeline_counter.update(p.timeline_dataset_counts)
            period = p.timeline_period
        timeline_counter, period = cls._group_counter_if_needed(
            timeline_counter, period
        )

        # The period elements that are the same across all of them.
        # (it will be the period of the result)
        common_time_period = (
            list(periods[0].period_tuple[1:4]) if periods else [None] * 3
        )
        region_counter: Counter = Counter()

        for time_period in periods:
            region_counter.update(time_period.region_dataset_counts)

            # Attempt to fix broken geometries.
            # -> The 'high_tide_comp_20p' tests give an example of this: geometry is valid when
            #    created, but after serialisation+deserialisation become invalid due to float
            #    rounding.
            if (
                time_period.footprint_geometry
                and not time_period.footprint_geometry.is_valid
            ):
                _LOG.info("invalid_stored_geometry", summary=time_period.period_tuple)
                time_period.footprint_geometry = time_period.footprint_geometry.buffer(
                    0
                )

            # We're looking for the time period common to them all.
            # Strike out any elements that differ between our periods.
            this_period = time_period.period_tuple[1:4]
            for i, elem in enumerate(common_time_period):
                if elem is not None and (elem != this_period[i]):
                    # All following should be blank too, since this is a hierarchy.
                    _erase_elements_from(common_time_period, i)
                    break

        with_valid_geometries = [
            p
            for p in periods
            if p.footprint_count
            and p.footprint_geometry
            and p.footprint_geometry.is_valid
            and not p.footprint_geometry.is_empty
        ]

        geometry_union = _create_unified_footprint(
            with_valid_geometries, footprint_tolerance
        )
        total_datasets = sum(p.dataset_count for p in periods)

        # Non-null properties here are the ones that are the same across all inputs.
        year, month, day = common_time_period

        start_range = min(
            (r.time_range.begin for r in periods if r.time_range), default=None
        )
        end_range = max(
            (r.time_range.end for r in periods if r.time_range), default=None
        )
        return TimePeriodOverview(
            product_name=product_name,
            year=year,
            month=month,
            day=day,
            dataset_count=total_datasets,
            timeline_dataset_counts=timeline_counter,
            timeline_period=period,
            region_dataset_counts=region_counter,
            time_range=Range(start_range, end_range)
            if start_range and end_range
            else None,
            footprint_geometry=geometry_union,
            footprint_crs=footprint_crs,
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
            # Why choose the max version? Because we assume older ones didn't need to be replaced,
            # so the most recent refresh time is the version that we are current with.
            product_refresh_time=max(
                (p.product_refresh_time for p in periods), default=product_refresh_time
            ),
            summary_gen_time=min(
                (p.summary_gen_time for p in periods if p.summary_gen_time is not None),
                default=None,
            ),
            size_bytes=sum(p.size_bytes for p in periods if p.size_bytes is not None),
        )

    @property
    def footprint_wgs84(self) -> MultiPolygon | None:
        if not self.footprint_geometry:
            return None
        if not self.footprint_crs:
            warnings.warn(f"Geometry without a crs for {self}", stacklevel=2)
            return None

        return (
            Geometry(self.footprint_geometry, crs=self.footprint_crs)
            .to_crs("EPSG:4326", wrapdateline=True)
            .geom
        )

    @staticmethod
    def _group_counter_if_needed(counter: Counter, period: str):
        if len(counter) > 366:
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

    @property
    def footprint_srid(self) -> int | None:
        if self.footprint_crs is None:
            return None
        epsg = self.footprint_crs.lower()

        if not epsg.startswith("epsg:"):
            _LOG.warning("unsupported.to_srid", crs=self.footprint_crs)
            return None
        return int(epsg.split(":")[1])


def _erase_elements_from(items: list, start_i: int) -> list:
    """
    Erase from the given 'i' onward

    >>> _erase_elements_from([1, 2, 3], 0)
    [None, None, None]
    >>> _erase_elements_from([1, 2, 3], 1)
    [1, None, None]
    >>> _erase_elements_from([1, 2, 3], 2)
    [1, 2, None]
    >>> _erase_elements_from([1, 2, 3], 3)
    [1, 2, 3]
    """
    items[start_i:] = [None] * (len(items) - start_i)
    # Return the list just for convenience in doctest. It's actually mutable.
    return items


def _create_unified_footprint(
    with_valid_geometries: Sequence[TimePeriodOverview], footprint_tolerance: float
) -> BaseGeometry | None:
    """
    Union the given time period's footprints, trying to fix any invalid geometries.
    """
    if not with_valid_geometries:
        return None

    # TODO: transition away from bare shapely
    try:
        geometry_union = shapely.ops.unary_union(
            [p.footprint_geometry for p in with_valid_geometries]
        )
    except ValueError:
        # Attempt 2 at union: Exaggerate the overlap *slightly* to
        # avoid non-noded intersection.
        # TODO: does shapely have a snap-to-grid?
        try:
            _LOG.warning("summary.footprint.invalid_union", exc_info=True)
            geometry_union = shapely.ops.unary_union(
                [
                    p.footprint_geometry.buffer(0.001)
                    for p in with_valid_geometries
                    if p.footprint_geometry is not None
                ]
            )
        except ValueError:
            _LOG.warning("summary.footprint.invalid_buffered_union", exc_info=True)

            # Attempt 3 at union: Recursive filter bad polygons first
            polygonlist = _polygon_chain(with_valid_geometries)
            filtered_geom = _filter_geom(polygonlist)
            geometry_union = shapely.ops.unary_union(filtered_geom)

    if footprint_tolerance is not None:
        geometry_union = geometry_union.simplify(footprint_tolerance)

    return geometry_union


def _polygon_chain(valid_geometries: Iterable[TimePeriodOverview]) -> list:
    """Chain all the given [Mutli]Polygons into a single list."""
    polygonlist = []
    for poly in valid_geometries:
        if type(poly.footprint_geometry) is MultiPolygon:
            for p in list(poly.footprint_geometry):
                polygonlist.append(p)
        else:
            polygonlist.append(poly.footprint_geometry)
    return polygonlist


def _filter_geom(geomlist: list[BaseGeometry], start: int = 0) -> list[BaseGeometry]:
    """
    Recursive filtering of un-unionable polygons. Input list is modified in-place.
    Exhaustively searches for a run of polygons that cause a union error
    (eg. "non-noded intersection"), and cuts out the first one that it finds.
    """
    # Pass through empty lists
    if len(geomlist) == 0:
        return geomlist
    # Process non-empty lists
    if start == len(geomlist):
        geomlist.pop()
        return geomlist
    for i in range(len(geomlist) - start):
        try:
            shapely.ops.unary_union(geomlist[0 : i + start])
        except ValueError:
            del geomlist[i + start]
            start = start + i
            break
        if i == len(geomlist) - 1 - start:
            return geomlist
    _filter_geom(geomlist, start)
    return geomlist
