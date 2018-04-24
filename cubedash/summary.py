from __future__ import absolute_import

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, NamedTuple, Optional, Tuple

import dateutil.parser
import fiona
import pandas as pd
import shapely
import shapely.geometry
import shapely.ops
import structlog
from dateutil import tz
from shapely.geometry.base import BaseGeometry

from datacube.model import Dataset, DatasetType, Range

from . import _model
from . import _utils as utils
from ._model import cache, dataset_shape, index

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
    def add_periods(periods: Iterable["TimePeriodOverview"], group_by_month=False):
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
                datetime(date.year, date.month, 1).date() for date in counter.elements()
            )
            period = "month"

        with_valid_geometries = [
            p
            for p in periods
            if p.footprint_geometry
            and p.footprint_geometry.is_valid
            and not p.footprint_geometry.is_empty
        ]

        return TimePeriodOverview(
            sum(p.dataset_count for p in periods),
            counter,
            None,
            period,
            Range(
                min(r.time_range.begin for r in periods),
                max(r.time_range.end for r in periods),
            ),
            shapely.ops.unary_union(
                [p.footprint_geometry for p in with_valid_geometries]
            )
            if with_valid_geometries
            else None,
            sum(p.footprint_count for p in with_valid_geometries),
        )


def calculate_summary(product_name: str, time: Range) -> Optional[TimePeriodOverview]:
    log = _LOG.bind(product=product_name, time=time)
    log.debug("summary.calc")

    datasets = [
        (dataset, dataset_shape(dataset))
        for dataset in index.datasets.search(product=product_name, time=time)
    ]
    dataset_shapes = [
        shape
        for dataset, shape in datasets
        if shape and shape.is_valid and not shape.is_empty
    ]
    footprint_geometry = (
        shapely.ops.unary_union(dataset_shapes) if dataset_shapes else None
    )

    # Initialise all requested days as zero
    day_counts = Counter(
        {d.date(): 0 for d in pd.date_range(time.begin, time.end, closed="left")}
    )
    day_counts.update(
        _default_utc(dataset.center_time).astimezone(_model.GROUPING_TIME_ZONE).date()
        for dataset, shape in datasets
    )

    summary = TimePeriodOverview(
        len(datasets),
        day_counts,
        _datasets_to_feature(datasets)
        if 0 < len(dataset_shapes) < _model.MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY
        else None,
        "day",
        time,
        footprint_geometry,
        len(dataset_shapes),
    )
    log.debug(
        "summary.calc.done",
        dataset_count=summary.dataset_count,
        footprints_missing=summary.dataset_count - summary.footprint_count,
    )
    return summary


def _default_utc(d):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d


def _datasets_to_feature(datasets: Iterable[Tuple[Dataset, BaseGeometry]]):
    return {
        "type": "FeatureCollection",
        "features": [_dataset_to_feature(ds) for ds in datasets if ds[1]],
    }


def _dataset_to_feature(ds: Tuple[Dataset, BaseGeometry]):
    dataset, shape = ds
    return {
        "type": "Feature",
        "geometry": shape.__geo_interface__,
        "properties": {
            "id": str(dataset.id),
            "label": utils.dataset_label(dataset),
            "start_time": dataset.time.begin.isoformat(),
        },
    }


class SummaryStore:
    def put(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        summary: TimePeriodOverview,
    ):
        raise NotImplementedError("Write summary")

    def get(
        self, product_name: Optional[str], year: Optional[int], month: Optional[int]
    ) -> TimePeriodOverview:
        raise NotImplementedError("Get summary")

    def has(
        self, product_name: Optional[str], year: Optional[int], month: Optional[int]
    ) -> bool:
        return self.get(product_name, year, month) is not None

    def list_complete_products(self) -> Iterable[str]:
        all_products = index.datasets.types.get_all()
        existing_products = sorted(
            (
                product.name
                for product in all_products
                if self.has(product.name, None, None)
            )
        )
        return existing_products


class FileSummaryStore(SummaryStore):
    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path

    def put(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        summary: TimePeriodOverview,
    ):
        path = self._get_summary_path(product_name, year, month)

        # No subfolders for empty years/months
        if summary.dataset_count == 0 and (year or month):
            return

        if month is not None:
            path.mkdir(parents=True)

        self._summary_to_file(
            "-".join(str(s) for s in (product_name, year, month) if s), path, summary
        )

    def get(
        self, product_name: Optional[str], year: Optional[int], month: Optional[int]
    ) -> Optional[TimePeriodOverview]:
        path = self._get_summary_path(product_name, year, month)
        if not path.exists():
            return None

        return self._read_summary(path)

    def _get_summary_path(
        self,
        product_name: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ):
        path = self.base_path
        if product_name:
            path = path / product_name
        if year:
            path = path / ("%04d" % year)
        if month:
            path = path / ("%02d" % month)
        return path

    def _summary_to_file(self, name: str, path: Path, summary: TimePeriodOverview):
        schema = {"geometry": "Polygon", "properties": {"id": "str"}}
        with (path / "timeline.json").open("w") as f:
            json.dump(
                dict(
                    total_count=summary.dataset_count,
                    footprint_count=summary.footprint_count,
                    datasets_geojson=summary.datasets_geojson,
                    period=summary.period,
                    time_range=[
                        summary.time_range[0].isoformat(),
                        summary.time_range[1].isoformat(),
                    ]
                    if summary.time_range
                    else None,
                    series={d.isoformat(): v for d, v in summary.dataset_counts.items()}
                    if summary.dataset_counts
                    else None,
                ),
                f,
            )

        with fiona.open(
            str(path / "dataset-coverage.shp"), "w", "ESRI Shapefile", schema
        ) as f:
            if summary.footprint_geometry:
                f.write(
                    {
                        "geometry": shapely.geometry.mapping(
                            summary.footprint_geometry
                        ),
                        "properties": {"id": name},
                    }
                )

    def _read_summary(self, path: Path) -> TimePeriodOverview:
        with (path / "timeline.json").open("r") as f:
            timeline = json.load(f)

        coverage_path = path / "dataset-coverage.shp"

        with fiona.open(str(coverage_path)) as f:
            shapes = list(f)

        if not shapes:
            footprint = None
        else:
            if len(shapes) != 1:
                raise ValueError(
                    f"Unexpected number of shapes in coverage? {len(shapes)}"
                )

            footprint = shapely.geometry.shape(shapes[0]["geometry"])

        return TimePeriodOverview(
            dataset_count=timeline["total_count"],
            dataset_counts=Counter(
                {dateutil.parser.parse(d): v for d, v in timeline["series"].items()}
            )
            if timeline.get("series")
            else None,
            datasets_geojson=timeline.get("datasets_geojson"),
            period=timeline["period"],
            time_range=Range(
                dateutil.parser.parse(timeline["time_range"][0]),
                dateutil.parser.parse(timeline["time_range"][1]),
            )
            if timeline.get("time_range")
            else None,
            footprint_geometry=footprint,
            footprint_count=timeline["footprint_count"],
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(base_path={repr(self.base_path)})"


def write_total_summary(store: SummaryStore) -> TimePeriodOverview:
    """
    Write (or replace) the summary of all products that we've got data for.
    """
    products = store.list_complete_products()
    summary = TimePeriodOverview.add_periods(
        store.get(product_name, None, None) for product_name in products
    )
    store.put(None, None, None, summary)
    return summary


def write_product_summary(
    product: DatasetType, store: SummaryStore
) -> TimePeriodOverview:
    """
    Generate and write a summary of the given product
    """
    summaries = []
    for year in range(1985, datetime.today().year + 1):
        s = store.get(product.name, year, None)
        if s is None:
            s = _write_year_summary(product, year, store)

        summaries.append(s)

    summary = TimePeriodOverview.add_periods(summaries, group_by_month=True)
    store.put(product.name, None, None, summary)
    return summary


def _write_year_summary(
    product: DatasetType, year: int, store: SummaryStore
) -> TimePeriodOverview:
    summaries = []
    for month in range(1, 13):
        s = store.get(product.name, year, month)
        if s is None:
            s = _write_month_summary(product, year, month, store)

        summaries.append(s)

    summary = TimePeriodOverview.add_periods(summaries)
    store.put(product.name, year, None, summary)

    return summary


def _write_month_summary(
    product: DatasetType, year: int, month: int, store: SummaryStore
) -> TimePeriodOverview:
    summary = calculate_summary(product.name, utils.as_time_range(year, month))
    store.put(product.name, year, month, summary)
    return summary


## Web App instances ##


DEFAULT_STORE = FileSummaryStore(_model.SUMMARIES_DIR)


@cache.memoize(timeout=60)
def get_summary(
    product_name: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
) -> Optional[TimePeriodOverview]:
    # Days are small enough to calculate on the fly
    if year and month and day:
        return calculate_summary(product_name, utils.as_time_range(year, month, day))

    # Otherwise load from file
    return DEFAULT_STORE.get(product_name, year, month)


@cache.memoize(timeout=120)
def get_products_with_summaries() -> Iterable[Tuple[DatasetType, TimePeriodOverview]]:
    """
    The list of products that we have generated reports for.
    """

    products = [
        (index.products.get_by_name(product_name), get_summary(product_name))
        for product_name in DEFAULT_STORE.list_complete_products()
    ]
    if not products:
        raise RuntimeError(
            "No product reports. "
            "Run `python -m cubedash.generate --all` to generate some."
        )

    return products
