from __future__ import absolute_import

import functools
import json
import os
from collections import Counter
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import dateutil.parser
import fiona
import shapely
import shapely.geometry
import shapely.ops
import structlog
from geoalchemy2 import shape as geo_shape
from sqlalchemy import DDL, \
    and_
from sqlalchemy import func, select, null
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine
from cachetools.func import lru_cache

from cubedash import _utils
from cubedash.summary import _extents
from cubedash.summary._schema import DATASET_SPATIAL, TIME_OVERVIEW, PRODUCT, SPATIAL_REF_SYS
from datacube.drivers.postgres._schema import DATASET_TYPE
from datacube.index import Index
from datacube.model import Range
from cubedash.summary._summarise import TimePeriodOverview, SummaryStore
from cubedash.summary import _schema

_OUTPUT_CRS_EPSG = 4326

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

        # Otherwise the oldest summary that was generated
        overall_summary = self.get(None, None, None, None)
        if overall_summary:
            return overall_summary.summary_gen_time

        # Otherwise the creation time of our summary folder
        return datetime.fromtimestamp(os.path.getctime(self.base_path))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(base_path={repr(self.base_path)})"


class PgSummaryStore(SummaryStore):

    def __init__(self, index: Index, log=_LOG) -> None:
        super().__init__(index, log)
        # The engine used for our own tables.
        # We may use our own engine in the future, as in many places the original
        # datacube is read-only.
        # pylint: disable=protected-access
        self._engine: Engine = index._db._engine

    def init(self):
        _schema.METADATA.create_all(self._engine, checkfirst=True)
        _extents.add_spatial_table(
            self._index, *self._index.products.get_all()
        )

    def drop_all(self):
        """
        Drop all cubedash-specific tables/schema.
        """
        self._engine.execute(
            DDL(f'drop schema if exists {_schema.CUBEDASH_SCHEMA} cascade')
        )

    @lru_cache(1)
    def _target_srid(self):
        # The pre-populated srid primary keys in postgis all default to the epsg code,
        # but we'll do the lookup anyway to be a good citizen.
        return self._engine.execute(
            select([SPATIAL_REF_SYS.c.srid])
                .where(SPATIAL_REF_SYS.c.auth_name == 'EPSG')
                .where(SPATIAL_REF_SYS.c.auth_srid == _OUTPUT_CRS_EPSG)
        ).scalar()

    def calculate_summary(self,
                          product_name: str,
                          time: Range) -> TimePeriodOverview:
        """
        Create a summary of the given product/time range.

        Default implementation uses the pure index api.
        """
        log = self._log.bind(product_name=product_name, time=time)
        log.debug("summary.query")

        result = self._engine.execute(
            select((
                func.ST_SRID(DATASET_SPATIAL.c.footprint).label('srid'),
                func.count().label("dataset_count"),
                func.ST_Transform(
                    func.ST_Union(DATASET_SPATIAL.c.footprint),
                    self._target_srid()
                ).label("footprint_geometry"),
                func.max(DATASET_SPATIAL.c.creation_time).label("newest_dataset_creation_time"),
                func.jsonb_agg(
                    func.jsonb_build_object(
                        # TODO: move ID to outer id field?
                        'type', 'Feature',
                        'geometry', func.ST_AsGeoJSON(DATASET_SPATIAL.c.footprint).cast(postgres.JSONB),
                        'properties', func.jsonb_build_object(
                            'id', DATASET_SPATIAL.c.id,
                            # TODO: dataset label?
                            'start_time', func.lower(DATASET_SPATIAL.c.time),
                        ),
                    )
                ).label('datasets_geojson'),
                null().label("timeline_dataset_counts"),
            )).where(
                DATASET_SPATIAL.c.dataset_type_ref == select([DATASET_TYPE.c.id]).where(
                    DATASET_TYPE.c.name == product_name)
            ).where(
                DATASET_SPATIAL.c.time.overlaps(
                    func.tstzrange(
                        time.begin,
                        time.end,
                        type_=TSTZRANGE,
                    )
                )
            ).group_by('srid')
        )

        rows = result.fetchall()
        log.debug("summary.query.done", srid_rows=len(rows))

        log.debug("summary.calc")

        # Initialise all requested days as zero
        # day_counts = Counter({
        #     d.date(): 0 for d in pd.date_range(time.begin, time.end, closed='left')
        # })
        # day_counts.update(
        #     _utils.default_utc(dataset.center_time).astimezone(
        #         self.GROUPING_TIME_ZONE).date()
        #     for dataset, shape in datasets
        # )

        # TODO: self.MAX_DATASETS_TO_DISPLAY_INDIVIDUALLY

        # TODO: We're going to union the srid groups. Perhaps record stats per-srid?

        def convert_row(row):
            row = dict(row)
            row['footprint_geometry'] = geo_shape.to_shape(row['footprint_geometry'])
            return row

        srid_summaries = list(
            TimePeriodOverview(
                **convert_row(row),
                timeline_period='day',
                time_range=time,
                # TODO: filter invalid from the counts?
                footprint_count=row['dataset_count'],
            ) for row in rows
        )
        if len(srid_summaries) == 1:
            summary = srid_summaries[0]
        else:
            summary = TimePeriodOverview.add_periods(srid_summaries)
        log.debug(
            "summary.calc.done",
            dataset_count=summary.dataset_count,
            footprints_missing=summary.dataset_count - summary.footprint_count
        )
        return summary

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
            footprint_geometry=(
                None if res['footprint_geometry'] is None
                else geo_shape.to_shape(res['footprint_geometry'])
            ),
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

            footprint_geometry=(
                None if summary.footprint_geometry is None
                else geo_shape.from_shape(summary.footprint_geometry)
            ),
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


def _safe_read_date(d):
    if d:
        return _utils.default_utc(dateutil.parser.parse(d))

    return None
