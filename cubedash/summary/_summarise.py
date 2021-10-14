from collections import Counter
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import sqlalchemy
import structlog
from cachetools.func import lru_cache
from datacube.model import Range
from dateutil import tz
from geoalchemy2 import Geometry, shape as geo_shape
from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.sql import ColumnElement

from cubedash import _utils
from cubedash._utils import ODC_DATASET_TYPE
from cubedash.summary import TimePeriodOverview
from cubedash.summary._schema import (
    DATASET_SPATIAL,
    FOOTPRINT_SRID_EXPRESSION,
    get_srid_name,
)

_LOG = structlog.get_logger()


_NEWER_SQLALCHEMY = not sqlalchemy.__version__.startswith("1.3")


def _scalar_subquery(selectable):
    """
    Make select statement into a scalar subquery.

    We want to support SQLAlchemy 1.3 (which doesn't have `scalar_subquery()`,
    and avoid deprecation warnings on SQLAlchemy 1.4 (which wants you to use `scalar_subquery()`)
    """
    if _NEWER_SQLALCHEMY:
        return selectable.scalar_subquery()
    else:
        return selectable.as_scalar()


class Summariser:
    def __init__(self, engine, log=_LOG, grouping_time_zone="Australia/Darwin") -> None:
        self._engine = engine
        self.log = log
        # Group datasets using this timezone when counting them.
        # Aus data comes from Alice Springs
        self.grouping_time_zone = grouping_time_zone
        # cache
        self._grouping_time_zone_tz = tz.gettz(self.grouping_time_zone)

    def calculate_summary(
        self,
        product_name: str,
        year_month_day: Tuple[Optional[int], Optional[int], Optional[int]],
        product_refresh_time: datetime,
    ) -> TimePeriodOverview:
        """
        Create a summary of the given product/time range.
        """
        time = _utils.as_time_range(*year_month_day)
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
                        FOOTPRINT_SRID_EXPRESSION,
                        type_=Geometry(),
                    ).label("footprint_geometry"),
                    func.sum(DATASET_SPATIAL.c.size_bytes).label("size_bytes"),
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
                    func.sum(select_by_srid.c.size_bytes).label("size_bytes"),
                    func.ST_Union(
                        func.ST_Buffer(select_by_srid.c.footprint_geometry, 0),
                        type_=Geometry(),
                    ).label("footprint_geometry"),
                    func.max(select_by_srid.c.newest_dataset_creation_time).label(
                        "newest_dataset_creation_time"
                    ),
                    func.now().label("summary_gen_time"),
                )
            )
        )

        rows = result.fetchall()
        log.debug("summary.query.done", srid_rows=len(rows))

        assert len(rows) == 1
        row = dict(rows[0])
        row["dataset_count"] = int(row["dataset_count"]) if row["dataset_count"] else 0
        if row["footprint_geometry"] is not None:
            row["footprint_crs"] = self._get_srid_name(row["footprint_geometry"].srid)
            row["footprint_geometry"] = geo_shape.to_shape(row["footprint_geometry"])
        else:
            row["footprint_crs"] = None
        row["crses"] = None
        if row["srids"] is not None:
            row["crses"] = {self._get_srid_name(s) for s in row["srids"]}
        del row["srids"]

        # Convert from Python Decimal
        if row["size_bytes"] is not None:
            row["size_bytes"] = int(row["size_bytes"])

        has_data = row["dataset_count"] > 0

        log.debug("counter.calc")

        # Initialise all requested days as zero
        day_counts = Counter(
            {d.date(): 0 for d in pd.date_range(begin_time, end_time, closed="left")}
        )
        region_counts = Counter()
        if has_data:
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
                                        )(self.grouping_time_zone),
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
            region_counts = Counter(
                {
                    item: count
                    for item, count in self._engine.execute(
                        select(
                            [
                                DATASET_SPATIAL.c.region_code.label("region_code"),
                                func.count(),
                            ]
                        )
                        .where(where_clause)
                        .group_by("region_code")
                    )
                }
            )

        if product_refresh_time is None:
            raise RuntimeError(
                "Internal error: Newly-made time summaries should "
                "not have a null product refresh time."
            )

        year, month, day = year_month_day
        summary = TimePeriodOverview(
            **row,
            product_name=product_name,
            year=year,
            month=month,
            day=day,
            product_refresh_time=product_refresh_time,
            timeline_period="day",
            time_range=Range(begin_time, end_time),
            timeline_dataset_counts=day_counts,
            region_dataset_counts=region_counts,
            # TODO: filter invalid from the counts?
            footprint_count=row["dataset_count"] or 0,
        )

        log.debug(
            "summary.calc.done",
            dataset_count=summary.dataset_count,
            footprints_missing=summary.dataset_count - summary.footprint_count,
        )
        return summary

    def _with_default_tz(self, d: datetime) -> datetime:
        if d.tzinfo is None:
            return d.replace(tzinfo=self._grouping_time_zone_tz)
        return d

    def _where(
        self, product_name: str, time: Range
    ) -> Tuple[datetime, datetime, ColumnElement]:
        begin_time = self._with_default_tz(time.begin)
        end_time = self._with_default_tz(time.end)
        where_clause = and_(
            func.tstzrange(begin_time, end_time, "[]", type_=TSTZRANGE).contains(
                DATASET_SPATIAL.c.center_time
            ),
            DATASET_SPATIAL.c.dataset_type_ref
            == _scalar_subquery(
                select([ODC_DATASET_TYPE.c.id]).where(
                    ODC_DATASET_TYPE.c.name == product_name
                )
            ),
            or_(
                func.st_isvalid(DATASET_SPATIAL.c.footprint).is_(True),
                func.st_isvalid(DATASET_SPATIAL.c.footprint).is_(None),
            ),
        )
        return begin_time, end_time, where_clause

    @lru_cache()
    def _get_srid_name(self, srid: int):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        return get_srid_name(self._engine, srid)
