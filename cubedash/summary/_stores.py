from __future__ import absolute_import

from collections import Counter

import dateutil.parser
import fiona
import functools
import json
import os
import shapely
import shapely.geometry
import shapely.ops
import structlog
from datetime import datetime, date
from pathlib import Path
from sqlalchemy import ForeignKey, SmallInteger, MetaData, Enum, JSON, event, DDL, \
    select, and_, CheckConstraint
from sqlalchemy import Table, Column, Integer, String, DateTime, Date
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func
from geoalchemy2 import Geometry
from typing import Optional

from cubedash import _utils
from datacube.index import Index
from datacube.model import Range
from ._summarise import TimePeriodOverview, SummaryStore

_LOG = structlog.get_logger()


class FileSummaryStore(SummaryStore):

    def __init__(self, index: Index, base_path: Path) -> None:
        super().__init__(index)
        self.base_path = base_path

    def put(self,
            product_name: Optional[str],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],
            summary: TimePeriodOverview):
        path = self._get_summary_path(product_name, year, month)

        self._summary_to_file(
            "-".join(str(s) for s in
                     (product_name, year, month) if s),
            path,
            summary
        )

    def get(self,
            product_name: Optional[str],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]) -> Optional[TimePeriodOverview]:

        # Days are small enough to calculate on the fly
        if year and month and day:
            return self.update(product_name, year, month, day)

        path = self._get_summary_path(product_name, year, month)
        if not path.exists():
            return None

        return self._read_summary(path)

    def _get_summary_path(self,
                          product_name: Optional[str] = None,
                          year: Optional[int] = None,
                          month: Optional[int] = None):
        path = self.base_path
        if product_name:
            path = path / product_name
        if year:
            path = path / ('%04d' % year)
        if month:
            path = path / ('%02d' % month)
        return path

    @staticmethod
    def _summary_to_file(name: str,
                         path: Path,
                         summary: TimePeriodOverview):
        path.mkdir(parents=True, exist_ok=True)
        schema = {
            'geometry': 'Polygon',
            'properties': {'id': 'str'},
        }
        with (path / 'timeline.json').open('w') as f:
            json.dump(
                dict(
                    total_count=summary.dataset_count,
                    footprint_count=summary.footprint_count,
                    datasets_geojson=summary.datasets_geojson,
                    period=summary.timeline_period,
                    time_range=[
                        summary.time_range[0].isoformat(),
                        summary.time_range[1].isoformat()
                    ] if summary.time_range else None,
                    series={
                        d.isoformat(): v for d, v in summary.timeline_dataset_counts.items()
                    } if summary.timeline_dataset_counts else None,
                    generation_time=(
                        summary.summary_gen_time.isoformat()
                        if summary.summary_gen_time else None
                    ),
                    newest_dataset_creation_time=(
                        summary.newest_dataset_creation_time.isoformat()
                        if summary.newest_dataset_creation_time else None
                    ),
                ),
                f
            )

        with fiona.open(str(path / 'dataset-coverage.shp'), 'w', 'ESRI Shapefile',
                        schema) as f:
            if summary.footprint_geometry:
                f.write({
                    'geometry': shapely.geometry.mapping(summary.footprint_geometry),
                    'properties': {'id': name}
                })

    @staticmethod
    def _read_summary(path: Path) -> Optional[TimePeriodOverview]:
        timeline_path = path / 'timeline.json'
        coverage_path = path / 'dataset-coverage.shp'

        if not timeline_path.exists() or not coverage_path.exists():
            return None

        with timeline_path.open('r') as f:
            timeline = json.load(f)

        with fiona.open(str(coverage_path)) as f:
            shapes = list(f)

        if not shapes:
            footprint = None
        else:
            if len(shapes) != 1:
                raise ValueError(
                    f'Unexpected number of shapes in coverage? {len(shapes)}'
                )

            footprint = shapely.geometry.shape(shapes[0]['geometry'])

        return TimePeriodOverview(
            dataset_count=timeline['total_count'],
            timeline_dataset_counts=Counter(
                {dateutil.parser.parse(d): v for d, v in timeline['series'].items()}
            ) if timeline.get('series') else None,
            datasets_geojson=timeline.get('datasets_geojson'),
            timeline_period=timeline['period'],
            time_range=Range(
                dateutil.parser.parse(timeline['time_range'][0]),
                dateutil.parser.parse(timeline['time_range'][1])
            ) if timeline.get('time_range') else None,
            footprint_geometry=footprint,
            footprint_count=timeline['footprint_count'],
            newest_dataset_creation_time=_safe_read_date(
                timeline.get('newest_dataset_creation_time')
            ),
            summary_gen_time=_safe_read_date(timeline.get(
                'generation_time'
            )) or _utils.default_utc(
                datetime.fromtimestamp(os.path.getctime(timeline_path))
            ),
        )

    def get_last_updated(self) -> Optional[datetime]:
        """
        When was our data last updated?
        """
        # Does a file tell us when the database was last cloned?
        path = self.base_path / 'generated.txt'
        if path.exists():
            date_text = path.read_text()
            try:
                return dateutil.parser.parse(date_text)
            except ValueError:
                _LOG.warn("invalid.summary.generated.txt", text=date_text, path=path)

        # Otherwise the oldest summary that was generated
        overall_summary = self.get(None, None, None, None)
        if overall_summary:
            return overall_summary.summary_gen_time

        # Otherwise the creation time of our summary folder
        return datetime.fromtimestamp(os.path.getctime(self.base_path))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(base_path={repr(self.base_path)})"


def _safe_read_date(d):
    if d:
        return _utils.default_utc(dateutil.parser.parse(d))

    return None


CUBEDASH_SCHEMA = 'cubedash'
METADATA = MetaData(schema=CUBEDASH_SCHEMA)


PRODUCT = Table(
    'product', METADATA,

    Column('id', SmallInteger, primary_key=True, autoincrement=True),
    Column('name', String, unique=True, nullable=False),
)

TIME_OVERVIEW = Table(
    'time_overview', METADATA,
    # Uniquely identified by three values:
    Column('product_ref', None, ForeignKey(PRODUCT.c.id), primary_key=True),
    Column('start_day', Date, primary_key=True),
    Column('period_type', Enum('all', 'year', 'month', 'day', name='overviewperiod'),
           primary_key=True),

    Column('dataset_count', Integer, nullable=False),

    Column('timeline_dataset_start_days', postgres.ARRAY(DateTime(timezone=True))),
    Column('timeline_dataset_counts', postgres.ARRAY(Integer)),

    # Only when there's a small number of them.
    # GeoJSON featurecolleciton as it contains metadata per dataset (the id etc).
    Column('datasets_geojson', JSON, nullable=True),

    Column('timeline_period',
           Enum('year', 'month', 'week', 'day', name='timelineperiod')),

    # Frustrating that there's no default datetimetz range type by default in postgres
    Column('time_earliest', DateTime(timezone=True)),
    Column('time_latest', DateTime(timezone=True)),

    Column('footprint_geometry', Geometry("MULTIPOLYGON")),

    Column('footprint_count', Integer),

    # The most newly created dataset
    Column('newest_dataset_creation_time', DateTime(timezone=True)),

    # When this summary was generated
    Column(
        'generation_time',
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    ),

    CheckConstraint(
        r"array_length(timeline_dataset_start_days, 1) = "
        r"array_length(timeline_dataset_counts, 1)",
        name='timeline_lengths_equal'
    ),
)

event.listen(
    METADATA,
    'before_create',
    DDL(f"create schema if not exists {CUBEDASH_SCHEMA}")
)
event.listen(
    METADATA,
    'before_create',
    DDL(f"create extension if not exists postgis")
)


class PgSummaryStore(SummaryStore):

    def __init__(self, index: Index, log=_LOG) -> None:
        super().__init__(index, log)
        # The engine used for our own tables.
        # We may use our own engine in the future, as in many places the original
        # datacube is read-only.
        self._engine: Engine = index._db._engine

    def init(self):
        METADATA.create_all(self._engine, checkfirst=True)

    def drop_all(self):
        """
        Drop all cubedash-specific tables/schema.
        """
        self._engine.execute(
            DDL(f'drop schema if exists {CUBEDASH_SCHEMA} cascade')
        )

    def get(self, product_name: Optional[str], year: Optional[int],
            month: Optional[int], day: Optional[int]) -> Optional[TimePeriodOverview]:

        start_day, period = self._start_day(year, month, day)

        res = self._engine.execute(
            TIME_OVERVIEW.join(PRODUCT).select(
                and_(
                    PRODUCT.c.name == product_name,
                    TIME_OVERVIEW.c.start_day == start_day,
                    TIME_OVERVIEW.c.period_type == period,
                )
            )
        ).fetchone()

        if not res:
            return None

        return self._summary_from_row(res)

    def _start_day(self, year, month, day):
        period = 'all'
        if year:
            period = 'year'
        if month:
            period = 'month'
        if day:
            period = 'day'

        return date(year or 1900, month or 1, day or 1), period

    def _summary_from_row(self, res):

        timeline_dataset_counts = Counter(
            dict(
                zip(res['timeline_dataset_start_days'], res['timeline_dataset_counts']))
        ) if res['timeline_dataset_start_days'] else None

        return TimePeriodOverview(
            dataset_count=res['dataset_count'],
            # : Counter
            timeline_dataset_counts=timeline_dataset_counts,
            # GeoJSON FeatureCollection dict. But only when there's a small number of them.
            datasets_geojson=res['datasets_geojson'],
            timeline_period=res['timeline_period'],
            # : Range
            time_range=Range(res['time_earliest'], res['time_latest'])
            if res['time_earliest'] else None,
            # shapely.geometry.base.BaseGeometry
            footprint_geometry=res['footprint_geometry'],
            footprint_count=res['footprint_count'],
            # The most newly created dataset
            newest_dataset_creation_time=res['newest_dataset_creation_time'],
            # When this summary was last generated
            summary_gen_time=res['generation_time'],
        )

    def _summary_to_row(self, summary: TimePeriodOverview) -> dict:

        counts = summary.timeline_dataset_counts
        day_counts = day_values = None
        if counts:
            day_values, day_counts = zip(
                *sorted(summary.timeline_dataset_counts.items())
            )

        begin, end = summary.time_range if summary.time_range else (None, None)
        return dict(
            dataset_count=summary.dataset_count,
            timeline_dataset_start_days=day_values,
            timeline_dataset_counts=day_counts,

            datasets_geojson=summary.datasets_geojson,

            timeline_period=summary.timeline_period,

            time_earliest=begin,
            time_latest=end,

            footprint_geometry=summary.footprint_geometry,
            footprint_count=summary.footprint_count,

            newest_dataset_creation_time=summary.newest_dataset_creation_time,
            generation_time=summary.summary_gen_time
        )

    @functools.lru_cache()
    def _get_product_id(self, name: str):
        while True:
            # Select product, otherwise insert.

            row = self._engine.execute(
                select([PRODUCT.c.id]).where(PRODUCT.c.name == name)
            ).fetchone()

            if row:
                return row[0]

            # This insert may conflict if someone else added it in parallel,
            # hence the loop to select again.
            row = self._engine.execute(
                postgres.insert(
                    PRODUCT
                ).on_conflict_do_nothing(
                    index_elements=['name']
                ).values(
                    name=name
                )
            ).inserted_primary_key
            if row:
                return row[0]

    def put(self, product_name: Optional[str], year: Optional[int],
            month: Optional[int], day: Optional[int], summary: TimePeriodOverview):

        product_id = self._get_product_id(product_name)
        start_day, period = self._start_day(year, month, day)
        row = self._summary_to_row(summary)
        self._engine.execute(
            postgres.insert(TIME_OVERVIEW).on_conflict_do_update(
                index_elements=[
                    'product_ref', 'start_day', 'period_type'
                ],
                set_=row,
                where=and_(
                    TIME_OVERVIEW.c.product_ref == product_id,
                    TIME_OVERVIEW.c.start_day == start_day,
                    TIME_OVERVIEW.c.period_type == period,
                ),
            ).values(
                product_ref=product_id,
                start_day=start_day,
                period_type=period,
                **row
            )
        )
