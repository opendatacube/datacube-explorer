import warnings
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, List, Optional, Set, Tuple, Union

import shapely
import shapely.ops
import structlog
from datacube.model import Dataset, Range
from datacube.utils.geometry import Geometry
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry

_LOG = structlog.get_logger()


@dataclass
class TimePeriodOverview:
    # These four elements make up a pseudo-id of the time period we've summarised.
    #
    # -> None means "all"
    product_name: str
    year: Optional[int]
    month: Optional[int]
    day: Optional[int]

    dataset_count: int
    timeline_dataset_counts: Counter
    region_dataset_counts: Counter

    timeline_period: str

    time_range: Range

    footprint_geometry: Union[shapely.geometry.MultiPolygon, shapely.geometry.Polygon]
    footprint_crs: str

    footprint_count: int

    # The most newly created dataset
    newest_dataset_creation_time: datetime

    # List of CRSes that these datasets are in
    crses: Set[str]

    size_bytes: int

    # What version of our product table this was based on (the last_refresh_time on ProductSummary)
    product_refresh_time: datetime

    # When this summary was generated. Set on the server.
    summary_gen_time: datetime = None

    def __str__(self):
        return (
            f"{self.label} "
            f"({self.dataset_count} dataset{'s' if self.dataset_count > 1 else ''})"
        )

    @property
    def label(self):
        return " ".join([(str(p) if p else "all") for p in self.period_tuple])

    @property
    def period_tuple(self):
        """
        This is the pseudo-id of the product time period we've summarised.

        Any of them can be None to represent 'all'
        """
        return self.product_name, self.year, self.month, self.day

    @period_tuple.setter
    def period_tuple(self, v: Tuple[str, Optional[int], Optional[int], Optional[int]]):
        self.product_name, self.year, self.month, self.day = v

    def as_flat_period(self):
        """
        How we "flatten" the time-slice for storage in DB columns. Must remain stable!

        A "period type" enum, and a single date.
        """
        return self.flat_period_representation(self.year, self.month, self.day)

    @classmethod
    def flat_period_representation(
        cls, year: Optional[int], month: Optional[int], day: Optional[int]
    ) -> Tuple[str, datetime.date]:
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
    ) -> Tuple[int, int, int]:
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
    def empty(cls, product_name: str):
        p = cls.add_periods([])
        p.product_name = product_name
        return p

    @classmethod
    def add_periods(
        cls,
        periods: Iterable["TimePeriodOverview"],
        # This is in CRS units. Albers, so 1KM.
        # Lower value will have a more accurate footprint and much larger page load times.
        footprint_tolerance=1000.0,
    ):
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

        timeline_counter = Counter()
        for p in periods:
            timeline_counter.update(p.timeline_dataset_counts)
            period = p.timeline_period
        timeline_counter, period = cls._group_counter_if_needed(
            timeline_counter, period
        )

        # The period elements that are the same across all of them.
        # (it will be the period of the result)
        common_time_period = list(periods[0].period_tuple) if periods else ([None] * 4)
        region_counter = Counter()

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
            this_period = time_period.period_tuple
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
        product_name, year, month, day = common_time_period

        return TimePeriodOverview(
            product_name=product_name,
            year=year,
            month=month,
            day=day,
            dataset_count=total_datasets,
            timeline_dataset_counts=timeline_counter,
            timeline_period=period,
            region_dataset_counts=region_counter,
            time_range=Range(
                min(r.time_range.begin for r in periods) if periods else None,
                max(r.time_range.end for r in periods) if periods else None,
            ),
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
                (
                    p.product_refresh_time
                    for p in periods
                    if p.product_refresh_time is not None
                ),
                default=None,
            ),
            summary_gen_time=min(
                (p.summary_gen_time for p in periods if p.summary_gen_time is not None),
                default=None,
            ),
            size_bytes=sum(p.size_bytes for p in periods if p.size_bytes is not None),
        )

    @property
    def footprint_wgs84(self) -> Optional[MultiPolygon]:
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
    def _group_counter_if_needed(counter, period):
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
    def footprint_srid(self):
        if self.footprint_crs is None:
            return None
        epsg = self.footprint_crs.lower()

        if not epsg.startswith("epsg:"):
            _LOG.warning("unsupported.to_srid", crs=self.footprint_crs)
            return None
        return int(epsg.split(":")[1])


def _has_shape(datasets: Tuple[Dataset, Tuple[BaseGeometry, bool]]) -> bool:
    dataset, (shape, was_valid) = datasets
    return shape is not None


def _erase_elements_from(items: List, start_i: int):
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
    with_valid_geometries: List["TimePeriodOverview"], footprint_tolerance: float
):
    """
    Union the given time period's footprints, trying to fix any invalid geometries.
    """
    if not with_valid_geometries:
        return None

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
                [p.footprint_geometry.buffer(0.001) for p in with_valid_geometries]
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


def _filter_geom(geomlist: List[BaseGeometry], start=0) -> List[BaseGeometry]:
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
    else:
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
