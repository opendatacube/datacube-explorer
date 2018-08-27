from __future__ import absolute_import

import functools
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, Optional

import dateutil.parser
import structlog
from dateutil.tz import tz
from geoalchemy2 import shape as geo_shape
from sqlalchemy import DDL, String, and_, func, select
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine

from cubedash import _utils
from cubedash._utils import alchemy_engine
from cubedash.summary import TimePeriodOverview, _extents, _schema
from cubedash.summary._schema import DATASET_SPATIAL, PRODUCT, TIME_OVERVIEW
from cubedash.summary._summarise import Summariser
from datacube import utils as dc_utils
from datacube.index import Index
from datacube.model import Dataset, DatasetType, Range

_LOG = structlog.get_logger()


@dataclass
class ProductSummary:
    name: str
    dataset_count: int
    # Null when dataset_count == 0
    time_earliest: Optional[datetime]
    time_latest: Optional[datetime]

    # How long ago the spatial extents for this product were last refreshed.
    # (Field comes from DB on load)
    last_refresh_age: Optional[timedelta] = None

    id_: Optional[int] = None


class SummaryStore:
    def __init__(
        self, index: Index, summariser: Summariser, init_schema=False, log=_LOG
    ) -> None:
        self.index = index
        self.log = log
        self._update_listeners = []

        self._engine: Engine = alchemy_engine(index)
        self._summariser = summariser

        if init_schema:
            _schema.create_schema(self._engine)

    @classmethod
    def create(cls, index: Index, init_schema=False, log=_LOG) -> "SummaryStore":
        return cls(
            index, Summariser(alchemy_engine(index)), init_schema=init_schema, log=log
        )

    def close(self):
        """Close any pooled/open connections. Necessary before forking."""
        self.index.close()
        self._engine.dispose()

    def refresh_all_products(self, refresh_older_than: timedelta = timedelta(days=1)):
        for product in self.index.products.get_all():
            self.refresh_product(product, refresh_older_than=refresh_older_than)

    def refresh_product(
        self, product: DatasetType, refresh_older_than: timedelta = timedelta(days=1)
    ):
        our_product = self._get_product(product.name)

        if (
            our_product is not None
            and our_product.last_refresh_age < refresh_older_than
        ):
            _LOG.debug(
                "init.product.skip.too_recent",
                product_name=product.name,
                age=our_product.last_refresh_age,
            )
            return None

        _LOG.debug("init.product", product_name=product.name)
        added_count = _extents.refresh_product(self.index, product)
        earliest, latest, total_count = self._engine.execute(
            select(
                (
                    func.min(DATASET_SPATIAL.c.center_time),
                    func.max(DATASET_SPATIAL.c.center_time),
                    func.count(),
                )
            ).where(DATASET_SPATIAL.c.dataset_type_ref == product.id)
        ).fetchone()
        self._set_product_extent(
            ProductSummary(product.name, total_count, earliest, latest)
        )
        return added_count

    def drop_all(self):
        """
        Drop all cubedash-specific tables/schema.
        """
        self._engine.execute(
            DDL(f"drop schema if exists {_schema.CUBEDASH_SCHEMA} cascade")
        )

    def get(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
    ) -> Optional[TimePeriodOverview]:
        start_day, period = self._start_day(year, month, day)

        product = self._get_product(product_name)
        if not product:
            return None

        res = self._engine.execute(
            select([TIME_OVERVIEW]).where(
                and_(
                    TIME_OVERVIEW.c.product_ref == product.id_,
                    TIME_OVERVIEW.c.start_day == start_day,
                    TIME_OVERVIEW.c.period_type == period,
                )
            )
        ).fetchone()

        if not res:
            return None

        return _summary_from_row(res)

    def _start_day(self, year, month, day):
        period = "all"
        if year:
            period = "year"
        if month:
            period = "month"
        if day:
            period = "day"

        return date(year or 1900, month or 1, day or 1), period

    @functools.lru_cache()
    def _product(self, name: str) -> ProductSummary:
        row = self._engine.execute(
            select(
                [
                    PRODUCT.c.dataset_count,
                    PRODUCT.c.time_earliest,
                    PRODUCT.c.time_latest,
                    (func.now() - PRODUCT.c.last_refresh).label("last_refresh_age"),
                    PRODUCT.c.id.label("id_"),
                ]
            ).where(PRODUCT.c.name == name)
        ).fetchone()
        if not row:
            raise ValueError("Unknown product %r (initialised?)" % name)

        return ProductSummary(name=name, **row)

    def _get_product(self, name: str) -> Optional[ProductSummary]:
        try:
            return self._product(name)
        except ValueError:
            return None

    def _set_product_extent(self, product: ProductSummary):

        fields = dict(
            dataset_count=product.dataset_count,
            time_earliest=product.time_earliest,
            time_latest=product.time_latest,
            # Deliberately do all age calculations with the DB clock rather than local.
            last_refresh=func.now(),
        )
        row = self._engine.execute(
            postgres.insert(PRODUCT)
            .on_conflict_do_update(index_elements=["name"], set_=fields)
            .values(name=product.name, **fields)
        ).inserted_primary_key
        self._product.cache_clear()
        return row[0]

    def _put(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
        summary: TimePeriodOverview,
    ):
        product = self._product(product_name)
        start_day, period = self._start_day(year, month, day)
        row = _summary_to_row(summary)
        ret = self._engine.execute(
            postgres.insert(TIME_OVERVIEW)
            .returning(TIME_OVERVIEW.c.generation_time)
            .on_conflict_do_update(
                index_elements=["product_ref", "start_day", "period_type"],
                set_=row,
                where=and_(
                    TIME_OVERVIEW.c.product_ref == product.id_,
                    TIME_OVERVIEW.c.start_day == start_day,
                    TIME_OVERVIEW.c.period_type == period,
                ),
            )
            .values(
                product_ref=product.id_, start_day=start_day, period_type=period, **row
            )
        )
        [gen_time] = ret.fetchone()
        summary.summary_gen_time = gen_time

    def has(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
    ) -> bool:
        return self.get(product_name, year, month, day) is not None

    def get_dataset_footprints(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        limit: int = 500,
    ) -> Dict:
        """
        Return a GeoJSON FeatureCollection of each dataset footprint in the time range.

        Each Dataset is a separate GeoJSON Feature (with embedded properties for id and tile/grid).
        """
        if year:
            time_range = _utils.as_time_range(year, month, day)
        else:
            product = self._get_product(product_name)
            time_range = Range(product.time_earliest, product.time_latest)

        # Our table. Faster, but doesn't yet have some fields (labels etc). TODO
        # return self._summariser.get_dataset_footprints(
        #     product_name,
        #     time_range,
        #     limit
        # )

        datasets = self.index.datasets.search(
            limit=limit, product=product_name, time=time_range
        )
        return _datasets_to_feature(datasets)

    def get_or_update(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
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
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        generate_missing_children=True,
    ):
        """Update the given summary and return the new one"""
        product = self._product(product_name)
        get_child = self.get_or_update if generate_missing_children else self.get

        if year and month and day:
            # Don't store days, they're quick.
            return self._summariser.calculate_summary(
                product_name, _utils.as_time_range(year, month, day)
            )
        elif year and month:
            summary = self._summariser.calculate_summary(
                product_name, _utils.as_time_range(year, month)
            )
        elif year:
            summary = TimePeriodOverview.add_periods(
                get_child(product_name, year, month_, None) for month_ in range(1, 13)
            )
        elif product_name:
            if product.dataset_count > 0:
                years = range(product.time_earliest.year, product.time_latest.year + 1)
            else:
                years = []
            summary = TimePeriodOverview.add_periods(
                get_child(product_name, year_, None, None) for year_ in years
            )
        else:
            summary = TimePeriodOverview.add_periods(
                get_child(product.name, None, None, None)
                for product in self.index.products.get_all()
            )

        self._do_put(product_name, year, month, day, summary)

        for listener in self._update_listeners:
            listener(product_name, year, month, day, summary)
        return summary

    def _do_put(self, product_name, year, month, day, summary):

        # Don't bother storing empty periods that are outside of the existing range.
        # This doesn't have to be exact (note that someone may update in parallel too).
        if summary.dataset_count == 0 and (year or month):
            product = self._get_product(product_name)
            if (not product) or (not product.time_latest):
                return

            timezone = tz.gettz(self._summariser.grouping_time_zone)
            if (
                datetime(year, month or 1, day or 1, tzinfo=timezone)
                < product.time_earliest
            ):
                return
            if (
                datetime(year, month or 12, day or 28, tzinfo=timezone)
                > product.time_latest
            ):
                return

        self._put(product_name, year, month, day, summary)

    def list_complete_products(self) -> Iterable[str]:
        """
        List products with summaries available.
        """
        all_products = self.index.datasets.types.get_all()
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


def _safe_read_date(d):
    if d:
        return _utils.default_utc(dateutil.parser.parse(d))

    return None


def _summary_from_row(res):
    timeline_dataset_counts = (
        Counter(
            dict(
                zip(res["timeline_dataset_start_days"], res["timeline_dataset_counts"])
            )
        )
        if res["timeline_dataset_start_days"]
        else None
    )
    region_dataset_counts = (
        Counter(dict(zip(res["regions"], res["region_dataset_counts"])))
        if res["regions"]
        else None
    )

    return TimePeriodOverview(
        dataset_count=res["dataset_count"],
        # : Counter
        timeline_dataset_counts=timeline_dataset_counts,
        region_dataset_counts=region_dataset_counts,
        timeline_period=res["timeline_period"],
        # : Range
        time_range=Range(res["time_earliest"], res["time_latest"])
        if res["time_earliest"]
        else None,
        # shapely.geometry.base.BaseGeometry
        footprint_geometry=(
            None
            if res["footprint_geometry"] is None
            else geo_shape.to_shape(res["footprint_geometry"])
        ),
        footprint_crs=(
            None
            if res["footprint_geometry"] is None or res["footprint_geometry"].srid == -1
            else "EPSG:{}".format(res["footprint_geometry"].srid)
        ),
        size_bytes=res["size_bytes"],
        footprint_count=res["footprint_count"],
        # The most newly created dataset
        newest_dataset_creation_time=res["newest_dataset_creation_time"],
        # When this summary was last generated
        summary_gen_time=res["generation_time"],
        crses=set(res["crses"]) if res["crses"] is not None else None,
    )


def _summary_to_row(summary: TimePeriodOverview) -> dict:
    counts = summary.timeline_dataset_counts
    day_counts, day_values, region_counts, region_values = [], [], [], []
    if counts:
        day_values, day_counts = zip(*sorted(summary.timeline_dataset_counts.items()))
    if summary.region_dataset_counts:
        region_values, region_counts = zip(
            *sorted(summary.region_dataset_counts.items())
        )

    begin, end = summary.time_range if summary.time_range else (None, None)

    if summary.footprint_geometry and summary.footprint_srid is None:
        raise ValueError("Geometry without srid", summary)

    return dict(
        dataset_count=summary.dataset_count,
        timeline_dataset_start_days=day_values,
        timeline_dataset_counts=day_counts,
        # TODO: SQLALchemy needs a bit of type help for some reason. Possible PgGridCell bug?
        regions=func.cast(region_values, type_=postgres.ARRAY(String)),
        region_dataset_counts=region_counts,
        timeline_period=summary.timeline_period,
        time_earliest=begin,
        time_latest=end,
        size_bytes=summary.size_bytes,
        footprint_geometry=(
            None
            if summary.footprint_geometry is None
            else geo_shape.from_shape(
                summary.footprint_geometry, summary.footprint_srid
            )
        ),
        footprint_count=summary.footprint_count,
        generation_time=func.now(),
        newest_dataset_creation_time=summary.newest_dataset_creation_time,
        crses=summary.crses,
    )


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


def _datasets_to_feature(datasets: Iterable[Dataset]):
    return {
        "type": "FeatureCollection",
        "features": [_dataset_to_feature(ds_valid) for ds_valid in datasets],
    }


def _dataset_to_feature(dataset: Dataset):
    shape, valid_extent = _utils.dataset_shape(dataset)
    return {
        "type": "Feature",
        "geometry": shape.__geo_interface__,
        "properties": {
            "id": str(dataset.id),
            "label": _utils.dataset_label(dataset),
            "valid_extent": valid_extent,
            "start_time": dataset.time.begin.isoformat(),
            "creation_time": _dataset_created(dataset),
        },
    }
