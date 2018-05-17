from __future__ import absolute_import

from collections import Counter

import dateutil.parser
import fiona
import json
import os
import shapely
import shapely.geometry
import shapely.ops
import structlog
from datetime import datetime
from pathlib import Path
from typing import Optional

from cubedash import _utils
from datacube.index._api import Index

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

        # No subfolders for empty years/months
        if summary.dataset_count == 0 and (year or month):
            return

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
                    period=summary.period,
                    time_range=[
                        summary.time_range[0].isoformat(),
                        summary.time_range[1].isoformat()
                    ] if summary.time_range else None,
                    series={
                        d.isoformat(): v for d, v in summary.dataset_counts.items()
                    } if summary.dataset_counts else None,
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
            dataset_counts=Counter(
                {dateutil.parser.parse(d): v for d, v in timeline['series'].items()}
            ) if timeline.get('series') else None,
            datasets_geojson=timeline.get('datasets_geojson'),
            period=timeline['period'],
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
