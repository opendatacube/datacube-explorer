import warnings
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Set, Tuple, Union

import shapely
import shapely.ops
import structlog
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry

from datacube.model import Dataset, Range
from datacube.utils.geometry import Geometry

_LOG = structlog.get_logger()


@dataclass
class TimePeriodOverview:
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

    # When this summary was generated. Set on the server.
    summary_gen_time: datetime = None

    def __str__(self):
        return (
            f"{self.timeline_period}:{self.time_range.begin} "
            f"({self.dataset_count} datasets)"
        )

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
        crses = set(p.footprint_crs for p in periods)
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

        region_counter = Counter()
        for p in periods:
            region_counter.update(p.region_dataset_counts)

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
            # Attempt 2 at union: Exaggerate the overlap *slightly* to
            # avoid non-noded intersection.
            # TODO: does shapely have a snap-to-grid?
            # mpolygon = MultiPolygon([polygon for polygon in [p.footprint_geometry for p in with_valid_geometries]])
            try:
                _LOG.warn("summary.footprint.union", exc_info=True)
                geometry_union = (
                    shapely.ops.unary_union(
                        [
                            p.footprint_geometry.buffer(0.001)
                            for p in with_valid_geometries
                        ]
                    )
                    if with_valid_geometries
                    else None
                )
            except ValueError:
                _LOG.warn("summary.footprint.union.filtering", exc_info=True)

                # run recursive filter to keep a clean polygon list
                polygonlist = _polygon_chain(with_valid_geometries)
                filtered_geom = _filter_geom(polygonlist)
                geometry_union = (
                    shapely.ops.unary_union(filtered_geom)
                    if with_valid_geometries
                    else None
                )

        if footprint_tolerance is not None and geometry_union is not None:
            geometry_union = geometry_union.simplify(footprint_tolerance)

        total_datasets = sum(p.dataset_count for p in periods)

        return TimePeriodOverview(
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
            warnings.warn(f"Geometry without a crs for {self}")
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
            _LOG.warn("unsupported.to_srid", crs=self.footprint_crs)
            return None
        return int(epsg.split(":")[1])


def _has_shape(datasets: Tuple[Dataset, Tuple[BaseGeometry, bool]]) -> bool:
    dataset, (shape, was_valid) = datasets
    return shape is not None


# chain all the polygon within Multipolygon into a list
def _polygon_chain(valid_geometries):
    polygonlist = []
    for poly in valid_geometries:
        if type(poly.footprint_geometry) is MultiPolygon:
            for p in list(poly.footprint_geometry):
                polygonlist.append(p)
        else:
            polygonlist.append(poly.footprint_geometry)
    return polygonlist


# Recursive filtering for non-noded intersection
def _filter_geom(geomlist, start=0):
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
