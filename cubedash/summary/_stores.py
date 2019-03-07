from collections import Counter

import dateutil.parser
import functools
import re
import structlog
from dataclasses import dataclass
from datetime import date, timedelta
from datetime import datetime
from dateutil import tz
from geoalchemy2 import shape as geo_shape
from geoalchemy2.shape import to_shape
from shapely.geometry.base import BaseGeometry
from sqlalchemy import DDL
from sqlalchemy import and_, String
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select
from typing import Dict, Generator, Tuple, Optional, Sequence
from typing import Iterable
from typing import List
from uuid import UUID

from cubedash import _utils
from cubedash._utils import ODC_DATASET, ODC_DATASET_TYPE
from cubedash.summary import _extents, TimePeriodOverview
from cubedash.summary import _schema
from cubedash.summary._schema import DATASET_SPATIAL, TIME_OVERVIEW, PRODUCT, \
    SPATIAL_QUALITY_STATS
from cubedash.summary._schema import refresh_supporting_views
from cubedash.summary._summarise import Summariser
from datacube.index import Index
from datacube.model import Dataset
from datacube.model import DatasetType
from datacube.model import Range

_LOG = structlog.get_logger()


@dataclass
class ProductSummary:
    name: str
    dataset_count: int
    # Null when dataset_count == 0
    time_earliest: Optional[datetime]
    time_latest: Optional[datetime]

    source_products: List[str]
    derived_products: List[str]

    # How long ago the spatial extents for this product were last refreshed.
    # (Field comes from DB on load)
    last_refresh_age: Optional[timedelta] = None

    id_: Optional[int] = None


@dataclass
class DatasetItem:
    dataset_id: UUID
    bbox: object
    product_name: str
    geometry: BaseGeometry
    region_code: str
    creation_time: datetime
    center_time: datetime
    odc_dataset: Optional[Dataset] = None

    @property
    def geom_geojson(self) -> Optional[Dict]:
        if self.geometry is None:
            return None
        return self.geometry.__geo_interface__

    def as_geojson(self):
        return dict(
            id=self.dataset_id,
            type='Feature',
            bbox=self.bbox,
            geometry=self.geom_geojson,
            properties={
                'datetime': self.center_time,
                'odc:product': self.product_name,
                'odc:creation-time': self.creation_time,
                'cubedash:region_code': self.region_code,
            }
        )


class SummaryStore:
    def __init__(self, index: Index, summariser: Summariser, init_schema=False, log=_LOG) -> None:
        self.index = index
        self.log = log
        self._update_listeners = []

        self._engine: Engine = _utils.alchemy_engine(index)
        self._summariser = summariser

        if init_schema:
            _schema.create_schema(self._engine)

    @classmethod
    def create(cls, index: Index, init_schema=False, log=_LOG) -> 'SummaryStore':
        return cls(index,
                   Summariser(_utils.alchemy_engine(index)),
                   init_schema=init_schema,
                   log=log)

    def close(self):
        """Close any pooled/open connections. Necessary before forking."""
        self.index.close()
        self._engine.dispose()

    def refresh_all_products(self, refresh_older_than: timedelta = timedelta(days=1)):
        for product in self.all_dataset_types():
            self.refresh_product(product, refresh_older_than=refresh_older_than)
        self.refresh_stats()

    def refresh_product(self,
                        product: DatasetType,
                        refresh_older_than: timedelta = timedelta(hours=23),
                        dataset_sample_size: int = 1000):
        our_product = self.get_product_summary(product.name)

        if (our_product is not None and
                our_product.last_refresh_age < refresh_older_than):
            _LOG.debug(
                'init.product.skip.too_recent',
                product_name=product.name,
                age=str(our_product.last_refresh_age)
            )
            return None

        _LOG.debug('init.product', product_name=product.name)
        added_count = _extents.refresh_product(self.index, product)
        earliest, latest, total_count = self._engine.execute(
            select((
                func.min(DATASET_SPATIAL.c.center_time),
                func.max(DATASET_SPATIAL.c.center_time),
                func.count(),
            )).where(DATASET_SPATIAL.c.dataset_type_ref == product.id)
        ).fetchone()

        source_products = []
        derived_products = []
        if total_count:
            sample_percentage = min(dataset_sample_size / total_count, 1) * 100.0
            source_products = self._get_linked_products(product, kind='source', sample_percentage=sample_percentage)
            derived_products = self._get_linked_products(product, kind='derived', sample_percentage=sample_percentage)

        self._set_product_extent(
            ProductSummary(
                product.name,
                total_count,
                earliest,
                latest,
                source_products=source_products,
                derived_products=derived_products,
            )
        )
        return added_count

    def refresh_stats(self, concurrently=False):
        refresh_supporting_views(self._engine, concurrently=concurrently)

    def _get_linked_products(self, product, kind='source', sample_percentage=0.05):
        """
        Find products with upstream or downstream datasets from this product.

        It only samples a percentage of this product's datasets, due to slow speed. (But 1 dataset
        would be enough for most products)
        """
        if kind not in ('source', 'derived'):
            raise ValueError('Unexpected kind of link: %r' % kind)
        if not 0.0 < sample_percentage <= 100.0:
            raise ValueError('Sample percentage out of range 0>s>=100. Got %r' % sample_percentage)

        from_ref, to_ref = 'source_dataset_ref', 'dataset_ref'
        if kind == 'derived':
            to_ref, from_ref = from_ref, to_ref

        # Avoid tablesample (full table scan) when we're getting all of the product anyway.
        sample_sql = ""
        if sample_percentage < 100:
            sample_sql = "tablesample system (%(sample_percentage)s)"

        linked_product_names, = self._engine.execute(f"""
            with datasets as (
                select id from agdc.dataset {sample_sql} where dataset_type_ref=%(product_id)s and archived is null
            ), 
            linked_datasets as (
                select distinct {from_ref} as linked_dataset_ref from agdc.dataset_source inner join datasets d on d.id = {to_ref}
            ),
            linked_products as (
                select distinct dataset_type_ref from agdc.dataset inner join linked_datasets on id = linked_dataset_ref where archived is null
            ) 
            select array_agg(name order by name) from agdc.dataset_type inner join linked_products sp on id = dataset_type_ref;
        """, product_id=product.id, sample_percentage=sample_percentage).fetchone()

        _LOG.info(
            f"product.links.{kind}",
            product=product.name,
            linked=linked_product_names,
            sample_percentage=round(sample_percentage, 2)
        )
        return list(linked_product_names or [])

    def drop_all(self):
        """
        Drop all cubedash-specific tables/schema.
        """
        self._engine.execute(
            DDL(f'drop schema if exists {_schema.CUBEDASH_SCHEMA} cascade')
        )

    def get(self,
            product_name: Optional[str],
            year: Optional[int] = None,
            month: Optional[int] = None,
            day: Optional[int] = None) -> Optional[TimePeriodOverview]:
        start_day, period = self._start_day(year, month, day)

        product = self.get_product_summary(product_name)
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
        period = 'all'
        if year:
            period = 'year'
        if month:
            period = 'month'
        if day:
            period = 'day'

        return date(year or 1900, month or 1, day or 1), period

    # These are cached to avoid repeated unnecessary DB queries.
    @functools.lru_cache()
    def all_dataset_types(self) -> Iterable[DatasetType]:
        return tuple(self.index.products.get_all())

    @functools.lru_cache()
    def get_dataset_type(self, name) -> DatasetType:
        for d in self.all_dataset_types():
            if d.name == name:
                return d
        raise KeyError('Unknown dataset type %r' % name)

    @functools.lru_cache()
    def _dataset_type_by_id(self, id_) -> DatasetType:
        for d in self.all_dataset_types():
            if d.id == id_:
                return d
        raise KeyError('Unknown dataset type id %r' % id_)

    @functools.lru_cache()
    def _product(self, name: str) -> ProductSummary:
        row = self._engine.execute(
            select([
                PRODUCT.c.dataset_count,
                PRODUCT.c.time_earliest,
                PRODUCT.c.time_latest,
                (func.now() - PRODUCT.c.last_refresh).label("last_refresh_age"),
                PRODUCT.c.id.label("id_"),
                PRODUCT.c.source_product_refs,
                PRODUCT.c.derived_product_refs,
            ]).where(PRODUCT.c.name == name)
        ).fetchone()
        if not row:
            raise ValueError("Unknown product %r (initialised?)" % name)

        row = dict(row)
        source_products = [self._dataset_type_by_id(id_).name for id_ in row.pop('source_product_refs')]
        derived_products = [self._dataset_type_by_id(id_).name for id_ in row.pop('derived_product_refs')]

        return ProductSummary(
            name=name,
            source_products=source_products,
            derived_products=derived_products,
            **row
        )

    def get_quality_stats(self) -> Iterable[Dict]:
        stats = self._engine.execute(select([SPATIAL_QUALITY_STATS]))
        for row in stats:
            d = dict(row)
            d['product'] = self._dataset_type_by_id(row['dataset_type_ref'])
            d['avg_footprint_bytes'] = row['footprint_size'] / row['count'] if row['footprint_size'] else 0
            yield d

    def get_product_summary(self, name: str) -> Optional[ProductSummary]:
        try:
            return self._product(name)
        except ValueError:
            return None

    @property
    def grouping_timezone(self):
        """Timezone used for day/month/year grouping."""
        return tz.gettz(self._summariser.grouping_time_zone)

    def _set_product_extent(self, product: ProductSummary):
        source_product_ids = [self.index.products.get_by_name(name).id for name in product.source_products]
        derived_product_ids = [self.index.products.get_by_name(name).id for name in product.derived_products]
        fields = dict(
            dataset_count=product.dataset_count,
            time_earliest=product.time_earliest,
            time_latest=product.time_latest,

            source_product_refs=source_product_ids,
            derived_product_refs=derived_product_ids,
            # Deliberately do all age calculations with the DB clock rather than local.
            last_refresh=func.now(),
        )
        row = self._engine.execute(
            postgres.insert(
                PRODUCT
            ).on_conflict_do_update(
                index_elements=['name'],
                set_=fields
            ).values(
                name=product.name,
                **fields,
            )
        ).inserted_primary_key
        self._product.cache_clear()
        return row[0]

    def _put(self, product_name: Optional[str],
             year: Optional[int],
             month: Optional[int],
             day: Optional[int],
             summary: TimePeriodOverview):
        product = self._product(product_name)
        start_day, period = self._start_day(year, month, day)
        row = _summary_to_row(summary)
        ret = self._engine.execute(
            postgres.insert(
                TIME_OVERVIEW
            ).returning(
                TIME_OVERVIEW.c.generation_time
            ).on_conflict_do_update(
                index_elements=[
                    'product_ref', 'start_day', 'period_type'
                ],
                set_=row,
                where=and_(
                    TIME_OVERVIEW.c.product_ref == product.id_,
                    TIME_OVERVIEW.c.start_day == start_day,
                    TIME_OVERVIEW.c.period_type == period,
                ),
            ).values(
                product_ref=product.id_,
                start_day=start_day,
                period_type=period,
                **row
            )
        )
        [gen_time] = ret.fetchone()
        summary.summary_gen_time = gen_time

    def has(self,
            product_name: Optional[str],
            year: Optional[int] = None,
            month: Optional[int] = None,
            day: Optional[int] = None) -> bool:
        return self.get(product_name, year, month, day) is not None

    def get_item(self, ids: UUID, full_dataset: bool = True) -> Optional[DatasetItem]:
        """
        Get a DatasetItem record for the given dataset UUID if it exists.
        """
        items = list(self.search_items(dataset_ids=[ids], full_dataset=full_dataset))
        if not items:
            return None
        if len(items) > 1:
            raise RuntimeError(
                "Something is wrong: Multiple dataset results for a single UUID"
            )

        [item] = items
        return item

    def search_items(
            self, *,
            product_name: Optional[str] = None,
            time: Optional[Tuple[datetime, datetime]] = None,
            bbox: Tuple[float, float, float, float] = None,
            limit: int = 500,
            offset: int = 0,
            full_dataset: bool = False,
            dataset_ids: Sequence[UUID] = None,
            require_geometry=True,
    ) -> Generator[DatasetItem, None, None]:
        """
        Search datasets using Cubedash's spatial table

        Returned as DatasetItem records, with optional embedded full Datasets
        (if full_dataset==True)

        Returned results are always sorted by (center_time, id)
        """
        geom = func.ST_Transform(
            DATASET_SPATIAL.c.footprint,
            4326,
        )

        columns = [
            geom.label('geometry'),
            func.Box2D(geom).label('bbox'),
            # TODO: dataset label?
            DATASET_SPATIAL.c.region_code.label('region_code'),
            DATASET_SPATIAL.c.creation_time,
            DATASET_SPATIAL.c.center_time,
        ]

        # If fetching the whole dataset, we need to join the ODC dataset table.
        if full_dataset:
            query: Select = select((
                *columns, *_utils.DATASET_SELECT_FIELDS
            )).select_from(
                DATASET_SPATIAL.join(
                    ODC_DATASET,
                    onclause=ODC_DATASET.c.id == DATASET_SPATIAL.c.id
                )
            )
        # Otherwise query purely from the spatial table.
        else:
            query: Select = select(
                (
                    *columns,
                    DATASET_SPATIAL.c.id,
                    DATASET_SPATIAL.c.dataset_type_ref,
                )
            ).select_from(
                DATASET_SPATIAL
            )

        if time:
            where_clause = and_(
                func.tstzrange(
                    _utils.default_utc(time[0]),
                    _utils.default_utc(time[1]),
                    '[]',
                    type_=TSTZRANGE,
                ).contains(
                    DATASET_SPATIAL.c.center_time
                ),
            )
            query = query.where(where_clause)

        if bbox:
            query = query.where(
                func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326).intersects(
                    func.ST_MakeEnvelope(*bbox)
                )
            )

        if product_name:
            query = query.where(
                DATASET_SPATIAL.c.dataset_type_ref == select(
                    [ODC_DATASET_TYPE.c.id]
                ).where(
                    ODC_DATASET_TYPE.c.name == product_name
                )
            )

        if dataset_ids:
            query = query.where(DATASET_SPATIAL.c.id.in_(dataset_ids))

        if require_geometry:
            query = query.where(
                DATASET_SPATIAL.c.footprint != None
            )

        query = query.order_by(
            # We must ensure an order, as users request can request in pages.
            DATASET_SPATIAL.c.center_time, DATASET_SPATIAL.c.id
        ).limit(
            limit
        ).offset(
            # TODO: Offset/limit isn't particularly efficient for paging...
            offset
        )

        for r in self._engine.execute(query):
            shape = None
            if r.geometry is not None:
                shape = to_shape(r.geometry)
                # These shapes are valid in the db, but can become invalid on
                # reprojection.
                # (Eg. 32baf68c-7d91-4e13-8860-206ac69147b0)
                # (tests fail without this)
                if not shape.is_valid:
                    shape = shape.buffer(0)

            yield DatasetItem(
                dataset_id=r.id,
                bbox=_box2d_to_bbox(r.bbox) if r.bbox else None,
                product_name=self.index.products.get(r.dataset_type_ref).name,
                geometry=shape,
                region_code=r.region_code,
                creation_time=r.creation_time,
                center_time=r.center_time,
                odc_dataset=(
                    _utils.make_dataset_from_select_fields(self.index, r)
                    if full_dataset else None
                ),
            )

    def get_or_update(self,
                      product_name: Optional[str],
                      year: Optional[int] = None,
                      month: Optional[int] = None,
                      day: Optional[int] = None):
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

    def update(self,
               product_name: Optional[str],
               year: Optional[int] = None,
               month: Optional[int] = None,
               day: Optional[int] = None,
               generate_missing_children=True):
        """Update the given summary and return the new one"""
        product = self._product(product_name)
        get_child = self.get_or_update if generate_missing_children else self.get

        if year and month and day:
            # Don't store days, they're quick.
            return self._summariser.calculate_summary(
                product_name,
                _utils.as_time_range(year, month, day)
            )
        elif year and month:
            summary = self._summariser.calculate_summary(
                product_name,
                _utils.as_time_range(year, month),
            )
        elif year:
            summary = TimePeriodOverview.add_periods(
                get_child(product_name, year, month_, None)
                for month_ in range(1, 13)
            )
        elif product_name:
            if product.dataset_count > 0:
                years = range(product.time_earliest.year, product.time_latest.year + 1)
            else:
                years = []
            summary = TimePeriodOverview.add_periods(
                get_child(product_name, year_, None, None)
                for year_ in years
            )
        else:
            summary = TimePeriodOverview.add_periods(
                get_child(product.name, None, None, None)
                for product in self.all_dataset_types()
            )

        self._do_put(product_name, year, month, day, summary)

        for listener in self._update_listeners:
            listener(product_name, year, month, day, summary)
        return summary

    def _do_put(self, product_name, year, month, day, summary):
        log = _LOG.bind(product_name=product_name, time=(year, month, day), summary=summary)
        # Don't bother storing empty periods that are outside of the existing range.
        # This doesn't have to be exact (note that someone may update in parallel too).
        if summary.dataset_count == 0 and (year or month):
            product = self.get_product_summary(product_name)
            if (not product) or (not product.time_latest):
                log.debug("product.empty")
                return

            timezone = tz.gettz(self._summariser.grouping_time_zone)
            if datetime(year, month or 12, day or 28, tzinfo=timezone) < product.time_earliest:
                log.debug("product.skip.before_range")
                return
            if datetime(year, month or 1, day or 1, tzinfo=timezone) > product.time_latest:
                log.debug("product.skip.after_range")
                return
        log.debug("product.put")
        self._put(product_name, year, month, day, summary)

    def list_complete_products(self) -> Iterable[str]:
        """
        List products with summaries available.
        """
        all_products = self.all_dataset_types()
        existing_products = sorted(
            (
                product.name for product in all_products
                if self.has(product.name, None, None, None)
            )
        )
        return existing_products

    def get_last_updated(self) -> Optional[datetime]:
        """Time of last update, if known"""
        return None

    def find_datasets_for_region(self,
                                 product_name: str,
                                 region_code: str,
                                 year: int,
                                 month: int,
                                 day: int,
                                 limit: int) -> Iterable[Dataset]:

        time_range = _utils.as_time_range(
            year,
            month,
            day,
            tzinfo=self.grouping_timezone,
        )
        return _extents.datasets_by_region(self._engine, self.index, product_name, region_code, time_range, limit)


def _safe_read_date(d):
    if d:
        return _utils.default_utc(dateutil.parser.parse(d))

    return None


def _summary_from_row(res):
    timeline_dataset_counts = Counter(
        dict(
            zip(res['timeline_dataset_start_days'], res['timeline_dataset_counts']))
    ) if res['timeline_dataset_start_days'] else None
    region_dataset_counts = Counter(
        dict(
            zip(res['regions'], res['region_dataset_counts']))
    ) if res['regions'] else None

    return TimePeriodOverview(
        dataset_count=res['dataset_count'],
        # : Counter
        timeline_dataset_counts=timeline_dataset_counts,
        region_dataset_counts=region_dataset_counts,
        timeline_period=res['timeline_period'],
        # : Range
        time_range=Range(res['time_earliest'], res['time_latest'])
        if res['time_earliest'] else None,
        # shapely.geometry.base.BaseGeometry
        footprint_geometry=(
            None if res['footprint_geometry'] is None
            else geo_shape.to_shape(res['footprint_geometry'])
        ),
        footprint_crs=(
            None if res['footprint_geometry'] is None or res['footprint_geometry'].srid == -1
            else 'EPSG:{}'.format(res['footprint_geometry'].srid)
        ),
        size_bytes=res['size_bytes'],
        footprint_count=res['footprint_count'],
        # The most newly created dataset
        newest_dataset_creation_time=res['newest_dataset_creation_time'],
        # When this summary was last generated
        summary_gen_time=res['generation_time'],
        crses=set(res['crses']) if res['crses'] is not None else None,
    )


def _summary_to_row(summary: TimePeriodOverview) -> dict:
    day_values, day_counts = _counter_key_vals(summary.timeline_dataset_counts)
    region_values, region_counts = _counter_key_vals(summary.region_dataset_counts)

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
            None if summary.footprint_geometry is None
            else geo_shape.from_shape(summary.footprint_geometry, summary.footprint_srid)
        ),
        footprint_count=summary.footprint_count,

        generation_time=func.now(),

        newest_dataset_creation_time=summary.newest_dataset_creation_time,
        crses=summary.crses
    )


def _counter_key_vals(counts: Counter) -> Tuple[Tuple, Tuple]:
    """
    Split counter into a keys sequence and a values sequence.

    (Both sorted by key)

    >>> tuple(_counter_key_vals(Counter(['a', 'a', 'b'])))
    (('a', 'b'), (2, 1))
    >>> tuple(_counter_key_vals(Counter(['a'])))
    (('a',), (1,))
    >>> # Important! zip(*) doesn't do this.
    >>> tuple(_counter_key_vals(Counter()))
    ((), ())
    """
    items = sorted(counts.items())
    return tuple(k for k, v in items), tuple(v for k, v in items)


def _datasets_to_feature(datasets: Iterable[Dataset]):
    return {
        'type': 'FeatureCollection',
        'features': [_dataset_to_feature(ds_valid) for ds_valid in datasets]
    }


def _dataset_to_feature(dataset: Dataset):
    shape, valid_extent = _utils.dataset_shape(dataset)
    return {
        'type': 'Feature',
        'geometry': shape.__geo_interface__,
        'properties': {
            'id': str(dataset.id),
            'label': _utils.dataset_label(dataset),
            'valid_extent': valid_extent,
            'start_time': dataset.time.begin.isoformat(),
            'creation_time': _utils.dataset_created(dataset),
        }
    }


_BOX2D_PATTERN = re.compile(
    r"BOX\(([-0-9.]+)\s+([-0-9.]+)\s*,\s*([-0-9.]+)\s+([-0-9.]+)\)"
)


def _box2d_to_bbox(pg_box2d: str) -> Tuple[float, float, float, float]:
    """
    Parse Postgis's box2d to a geojson/stac bbox tuple.

    >>> _box2d_to_bbox(
    ...     "BOX(134.806923200497 -17.7694714883835,135.769692610214 -16.8412669214876)"
    ... )
    (134.806923200497, -17.7694714883835, 135.769692610214, -16.8412669214876)
    """
    m = _BOX2D_PATTERN.match(pg_box2d)
    # We know there's exactly four groups, but type checker doesn't...
    # noinspection PyTypeChecker
    return tuple(float(m) for m in m.groups())
