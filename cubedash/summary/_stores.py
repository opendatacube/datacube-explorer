from __future__ import absolute_import

import functools
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Optional

import dateutil.parser
import pandas as pd
import structlog
from cachetools.func import lru_cache
from dateutil import tz
from geoalchemy2 import Geometry
from geoalchemy2 import shape as geo_shape
from sqlalchemy import DDL, Integer, String, and_, bindparam, func, select
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine

from cubedash import _utils
from cubedash._utils import alchemy_engine
from cubedash.summary import TimePeriodOverview, _extents, _schema
from cubedash.summary._schema import (
    DATASET_SPATIAL,
    PRODUCT,
    SPATIAL_REF_SYS,
    TIME_OVERVIEW,
    PgGridCell,
)
from datacube.drivers.postgres._schema import DATASET_TYPE
from datacube.index import Index
from datacube.model import DatasetType, Range

_LOG = structlog.get_logger()


@dataclass
class ProductSummary:
    name: str
    dataset_count: int
    # Null when dataset_count == 0
    time_earliest: Optional[datetime]
    time_latest: Optional[datetime]

    id_: Optional[int] = None


class SummaryStore:
    def __init__(self, index: Index, log=_LOG) -> None:
        self.index = index
        self.log = log
        self._update_listeners = []

        self._engine: Engine = alchemy_engine(index)

    # Group datasets using this timezone when counting them.
    # Aus data comes from Alice Springs
    GROUPING_TIME_ZONE_NAME = "Australia/Darwin"
    # cache
    GROUPING_TIME_ZONE_TZ = tz.gettz(GROUPING_TIME_ZONE_NAME)

    OUTPUT_CRS_EPSG_CODE = 4326

    # If there's fewer than this many datasets, display them as individual polygons in
    # the browser. Too many can bog down the browser's performance.
    # (Otherwise dataset footprint is shown as a single composite polygon)
    MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY = 600

    def init(self, init_products=True):
        _schema.METADATA.create_all(self._engine, checkfirst=True)
        if init_products:
            for product in self.index.products.get_all():
                _LOG.debug("init.product", product_name=product.name)
                self.init_product(product)

    def init_product(self, product: DatasetType):
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

    @lru_cache(1)
    def _target_srid(self):
        """
        Get the srid key for our target CRS (that all geometry is returned as)

        The pre-populated srid primary keys in postgis all default to the epsg code,
        but we'll do the lookup anyway to be good citizens.
        """
        return self._engine.execute(
            select([SPATIAL_REF_SYS.c.srid])
            .where(SPATIAL_REF_SYS.c.auth_name == "EPSG")
            .where(SPATIAL_REF_SYS.c.auth_srid == self.OUTPUT_CRS_EPSG_CODE)
        ).scalar()

    @lru_cache()
    def _get_srid_name(self, srid):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        return self._engine.execute(
            select(
                [
                    func.concat(
                        SPATIAL_REF_SYS.c.auth_name,
                        ":",
                        SPATIAL_REF_SYS.c.auth_srid.cast(Integer),
                    )
                ]
            ).where(SPATIAL_REF_SYS.c.srid == bindparam("srid", srid, type_=Integer))
        ).scalar()

    def _get_datasets_geojson(self, where_clause):
        return self._engine.execute(
            select(
                [
                    func.jsonb_build_object(
                        "type",
                        "FeatureCollection",
                        "features",
                        func.jsonb_agg(
                            func.jsonb_build_object(
                                # TODO: move ID to outer id field?
                                "type",
                                "Feature",
                                "geometry",
                                func.ST_AsGeoJSON(
                                    func.ST_Transform(
                                        DATASET_SPATIAL.c.footprint, self._target_srid()
                                    )
                                ).cast(postgres.JSONB),
                                "properties",
                                func.jsonb_build_object(
                                    "id",
                                    DATASET_SPATIAL.c.id,
                                    # TODO: dataset label?
                                    "grid_point",
                                    DATASET_SPATIAL.c.grid_point.cast(String),
                                    "creation_time",
                                    DATASET_SPATIAL.c.creation_time,
                                    "center_time",
                                    DATASET_SPATIAL.c.center_time,
                                ),
                            )
                        ),
                    ).label("datasets_geojson")
                ]
            ).where(where_clause)
        ).fetchone()["datasets_geojson"]

    def _with_default_tz(self, d: datetime) -> datetime:
        if d.tzinfo is None:
            return d.replace(tzinfo=self.GROUPING_TIME_ZONE_TZ)
        return d

    def calculate_summary(self, product_name: str, time: Range) -> TimePeriodOverview:
        """
        Create a summary of the given product/time range.

        Default implementation uses the pure index api.
        """
        print(f"Gen {product_name}, {repr(time)}")
        log = self.log.bind(product_name=product_name, time=time)
        log.debug("summary.query")

        begin_time, end_time, where_clause = self._where(product_name, time)
        select_by_srid = (
            select(
                (
                    func.ST_SRID(DATASET_SPATIAL.c.footprint).label("srid"),
                    func.count().label("dataset_count"),
                    func.ST_Transform(
                        func.ST_Union(DATASET_SPATIAL.c.footprint),
                        self._target_srid(),
                        type_=Geometry(),
                    ).label("footprint_geometry"),
                    func.max(DATASET_SPATIAL.c.creation_time).label(
                        "newest_dataset_creation_time"
                    ),
                )
            )
            .where(where_clause)
            .group_by("srid")
            .alias("srid_summaries")
        )

        # Union all srid groups into one summary.
        result = self._engine.execute(
            select(
                (
                    func.sum(select_by_srid.c.dataset_count).label("dataset_count"),
                    func.array_agg(select_by_srid.c.srid).label("srids"),
                    func.ST_Union(
                        select_by_srid.c.footprint_geometry, type_=Geometry()
                    ).label("footprint_geometry"),
                    func.max(select_by_srid.c.newest_dataset_creation_time).label(
                        "newest_dataset_creation_time"
                    ),
                )
            )
        )

        rows = result.fetchall()
        log.debug("summary.query.done", srid_rows=len(rows))

        assert len(rows) == 1
        row = dict(rows[0])
        row["dataset_count"] = row["dataset_count"] or 0
        if row["footprint_geometry"] is not None:
            row["footprint_geometry"] = geo_shape.to_shape(row["footprint_geometry"])
        row["crses"] = None
        if row["srids"] is not None:
            row["crses"] = {self._get_srid_name(s) for s in row["srids"]}
        del row["srids"]

        has_data = row["dataset_count"] > 0

        log.debug("counter.calc")

        # Initialise all requested days as zero

        if not has_data:
            grid_counts = Counter()
            day_counts = Counter()
        else:
            day_counts = Counter(
                {
                    d.date(): 0
                    for d in pd.date_range(begin_time, end_time, closed="left")
                }
            )
            day_counts.update(
                Counter(
                    {
                        day.date(): count
                        for day, count in self._engine.execute(
                            select(
                                [
                                    func.date_trunc(
                                        "day",
                                        DATASET_SPATIAL.c.center_time.op(
                                            "AT TIME ZONE"
                                        )(self.GROUPING_TIME_ZONE_NAME),
                                    ).label("day"),
                                    func.count(),
                                ]
                            )
                            .where(where_clause)
                            .group_by("day")
                        )
                    }
                )
            )
            grid_counts = Counter(
                {
                    item: count
                    for item, count in self._engine.execute(
                        select(
                            [
                                DATASET_SPATIAL.c.grid_point.label("grid_point"),
                                func.count(),
                            ]
                        )
                        .where(where_clause)
                        .group_by("grid_point")
                    )
                }
            )

        summary = TimePeriodOverview(
            **row,
            timeline_period="day",
            time_range=time,
            timeline_dataset_counts=day_counts,
            grid_dataset_counts=grid_counts,
            # TODO: filter invalid from the counts?
            footprint_count=row["dataset_count"] or 0,
        )

        log.debug(
            "summary.calc.done",
            dataset_count=summary.dataset_count,
            footprints_missing=summary.dataset_count - summary.footprint_count,
        )
        return summary

    def _where(self, product_name, time):
        begin_time = self._with_default_tz(time.begin)
        end_time = self._with_default_tz(time.end)
        where_clause = and_(
            func.tstzrange(begin_time, end_time, "[]", type_=TSTZRANGE).contains(
                DATASET_SPATIAL.c.center_time
            ),
            DATASET_SPATIAL.c.dataset_type_ref
            == select([DATASET_TYPE.c.id]).where(DATASET_TYPE.c.name == product_name),
        )
        return begin_time, end_time, where_clause

    def get(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
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

        return self._summary_from_row(res)

    def _start_day(self, year, month, day):
        period = "all"
        if year:
            period = "year"
        if month:
            period = "month"
        if day:
            period = "day"

        return date(year or 1900, month or 1, day or 1), period

    def _summary_from_row(self, res):

        timeline_dataset_counts = (
            Counter(
                dict(
                    zip(
                        res["timeline_dataset_start_days"],
                        res["timeline_dataset_counts"],
                    )
                )
            )
            if res["timeline_dataset_start_days"]
            else None
        )
        grid_dataset_counts = (
            Counter(dict(zip(res["grid_dataset_grids"], res["grid_dataset_counts"])))
            if res["grid_dataset_grids"]
            else None
        )

        return TimePeriodOverview(
            dataset_count=res["dataset_count"],
            # : Counter
            timeline_dataset_counts=timeline_dataset_counts,
            grid_dataset_counts=grid_dataset_counts,
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
            footprint_count=res["footprint_count"],
            # The most newly created dataset
            newest_dataset_creation_time=res["newest_dataset_creation_time"],
            # When this summary was last generated
            summary_gen_time=res["generation_time"],
            crses=set(res["crses"]) if res["crses"] is not None else None,
        )

    def _summary_to_row(self, summary: TimePeriodOverview) -> dict:

        counts = summary.timeline_dataset_counts
        day_counts = day_values = grid_counts = grid_values = None
        if counts:
            day_values, day_counts = zip(
                *sorted(summary.timeline_dataset_counts.items())
            )
            grid_values, grid_counts = zip(*sorted(summary.grid_dataset_counts.items()))

        begin, end = summary.time_range if summary.time_range else (None, None)
        return dict(
            dataset_count=summary.dataset_count,
            timeline_dataset_start_days=day_values,
            timeline_dataset_counts=day_counts,
            # TODO: SQLALchemy needs a bit of type help for some reason. Possible PgGridCell bug?
            grid_dataset_grids=func.cast(grid_values, type_=postgres.ARRAY(PgGridCell)),
            grid_dataset_counts=grid_counts,
            timeline_period=summary.timeline_period,
            time_earliest=begin,
            time_latest=end,
            footprint_geometry=(
                None
                if summary.footprint_geometry is None
                else geo_shape.from_shape(summary.footprint_geometry)
            ),
            footprint_count=summary.footprint_count,
            newest_dataset_creation_time=summary.newest_dataset_creation_time,
            generation_time=summary.summary_gen_time,
            crses=summary.crses,
        )

    @functools.lru_cache()
    def _get_product(self, name: str) -> Optional[ProductSummary]:
        row = self._engine.execute(
            select(
                [
                    PRODUCT.c.dataset_count,
                    PRODUCT.c.time_earliest,
                    PRODUCT.c.time_latest,
                    PRODUCT.c.id.label("id_"),
                ]
            ).where(PRODUCT.c.name == name)
        ).fetchone()
        if row:
            return ProductSummary(name=name, **row)
        else:
            return None

    def _set_product_extent(self, product: ProductSummary):
        # This insert may conflict if someone else added it in parallel,
        # hence the loop to select again.
        row = self._engine.execute(
            postgres.insert(PRODUCT)
            .on_conflict_do_update(
                index_elements=["name"],
                set_=dict(
                    dataset_count=product.dataset_count,
                    time_earliest=product.time_earliest,
                    time_latest=product.time_latest,
                ),
            )
            .values(
                name=product.name,
                dataset_count=product.dataset_count,
                time_earliest=product.time_earliest,
                time_latest=product.time_latest,
            )
        ).inserted_primary_key
        return row[0]

    def _put(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
        summary: TimePeriodOverview,
    ):
        product = self._get_product(product_name)
        if not product:
            raise ValueError("Unknown product %r" % product_name)

        start_day, period = self._start_day(year, month, day)
        row = self._summary_to_row(summary)
        self._engine.execute(
            postgres.insert(TIME_OVERVIEW)
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

    def has(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
    ) -> bool:
        return self.get(product_name, year, month, day) is not None

    def get_datasets_geojson(
        self,
        product_name: Optional[str],
        year: Optional[int],
        month: Optional[int],
        day: Optional[int],
    ):
        begin_time, end_time, where_clause = self._where(
            product_name, _utils.as_time_range(year, month, day)
        )
        return self._get_datasets_geojson(where_clause)

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
        product = self._get_product(product_name)
        if not product:
            raise ValueError("Unknown product (initialised?)")

        get_child = self.get_or_update if generate_missing_children else self.get

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
            product_extent = self.get(product_name, None, None, None)
            if (not product_extent) or (not product_extent.time_range):
                return

            start, end = product_extent.time_range
            if datetime(year, month or 1, day or 1) < start:
                return
            if datetime(year, month or 12, day or 28) > end:
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
