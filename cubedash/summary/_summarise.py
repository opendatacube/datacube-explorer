import os
from collections import Counter
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import sqlalchemy
import structlog
from datacube.model import Range
from dateutil import tz
from geoalchemy2 import shape as geo_shape

from cubedash import _utils
from cubedash.summary import TimePeriodOverview

_LOG = structlog.get_logger()


_NEWER_SQLALCHEMY = not sqlalchemy.__version__.startswith("1.3")

# Get default timezone via CUBEDASH_SETTINGS specified config file if it exists,
# otherwise default to Australia/Darwin
default_timezone = "Australia/Darwin"
settings_file = os.environ.get("CUBEDASH_SETTINGS", "settings.env.py")
try:
    with open(os.path.join(os.getcwd(), settings_file)) as config_file:
        for line in config_file:
            val = line.rstrip().split("=")
            if val[0] == "CUBEDASH_DEFAULT_TIMEZONE":
                default_timezone = val[1]
except FileNotFoundError:
    pass
DEFAULT_TIMEZONE = default_timezone


class Summariser:
    def __init__(self, e_index, log=_LOG, grouping_time_zone=DEFAULT_TIMEZONE) -> None:
        self.e_index = e_index
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

        begin_time = self._with_default_tz(time.begin)
        end_time = self._with_default_tz(time.end)
        where_clause = self.e_index.summary_where_clause(
            product_name, begin_time, end_time
        )

        rows = self.e_index.srid_summary(where_clause).fetchall()
        log.debug("summary.query.done", srid_rows=len(rows))

        assert len(rows) == 1
        row = dict(rows[0]._mapping)
        row["dataset_count"] = int(row["dataset_count"]) if row["dataset_count"] else 0
        if row["footprint_geometry"] is not None:
            row["footprint_crs"] = self.e_index.get_srid_name(
                row["footprint_geometry"].srid
            )
            row["footprint_geometry"] = geo_shape.to_shape(row["footprint_geometry"])
        else:
            row["footprint_crs"] = None
        row["crses"] = None
        if row["srids"] is not None:
            row["crses"] = {self.e_index.get_srid_name(s) for s in row["srids"]}
        del row["srids"]

        # Convert from Python Decimal
        if row["size_bytes"] is not None:
            row["size_bytes"] = int(row["size_bytes"])

        has_data = row["dataset_count"] > 0

        log.debug("counter.calc")

        # Initialise all requested days as zero
        day_counts = Counter(
            {
                d.date(): 0
                for d in pd.date_range(
                    begin_time,
                    end_time,
                    inclusive="left",
                    nonexistent="shift_forward",
                )
            }
        )
        region_counts = Counter()
        if has_data:
            day_counts.update(
                Counter(
                    {
                        day.date(): count
                        for day, count in self.e_index.day_counts(
                            self.grouping_time_zone, where_clause
                        )
                    }
                )
            )
            region_counts = Counter(
                {
                    item: count
                    for item, count in self.e_index.region_counts(where_clause)
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
