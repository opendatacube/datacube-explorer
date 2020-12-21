import math
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import groupby
from typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from uuid import UUID

import dateutil.parser
import structlog
from cachetools.func import ttl_cache
from dateutil import tz
from geoalchemy2 import WKBElement
from geoalchemy2 import shape as geo_shape
from geoalchemy2.shape import to_shape
from sqlalchemy import DDL, String, and_, func, select
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine, RowProxy
from sqlalchemy.sql import Select

try:
    from .._version import version as EXPLORER_VERSION
except ModuleNotFoundError:
    EXPLORER_VERSION = "ci-test-pipeline"
from cubedash import _utils
from cubedash._utils import ODC_DATASET, ODC_DATASET_TYPE
from cubedash.summary import RegionInfo, TimePeriodOverview, _extents, _schema
from cubedash.summary._extents import RegionSummary, ProductArrival
from cubedash.summary._schema import (
    DATASET_SPATIAL,
    PRODUCT,
    REGION,
    SPATIAL_QUALITY_STATS,
    TIME_OVERVIEW,
    PleaseRefresh,
    get_srid_name,
    refresh_supporting_views,
)
from cubedash.summary._summarise import Summariser
from datacube import Datacube
from datacube.drivers.postgres._fields import PgDocField
from datacube.index import Index
from datacube.model import Dataset, DatasetType, Range
from datacube.utils.geometry import Geometry

DEFAULT_TTL = 10

_DEFAULT_REFRESH_OLDER_THAN = timedelta(hours=23)

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

    # Metadata values that are the same on every dataset.
    # (on large products this is judged via sampling, so may not be 100%)
    fixed_metadata: Dict[str, Union[str, float, int, datetime]]

    # How long ago the spatial extents for this product were last refreshed.
    # (Field comes from DB on load)
    last_refresh_age: Optional[timedelta] = None

    id_: Optional[int] = None


@dataclass
class DatasetItem:
    dataset_id: UUID
    bbox: object
    product_name: str
    geometry: Geometry
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
            type="Feature",
            bbox=self.bbox,
            geometry=self.geom_geojson,
            properties={
                "datetime": self.center_time,
                "odc:product": self.product_name,
                "odc:processing_datetime": self.creation_time,
                "cubedash:region_code": self.region_code,
            },
        )


@dataclass
class ProductLocationSample:
    """
    The apparent storage location of a product

    (judged via a small sampling of datasets)
    """

    # eg. 'http', "file", ...
    uri_scheme: str
    # The common uri prefix across all samples
    common_prefix: str
    # A few examples of full location URIs
    example_uris: List[str]


class SummaryStore:
    def __init__(self, index: Index, summariser: Summariser, log=_LOG) -> None:
        self.index = index
        self.log = log
        self._update_listeners = []

        self._engine: Engine = _utils.alchemy_engine(index)
        self._summariser = summariser

    def is_initialised(self) -> bool:
        """
        Do our DB schemas exist?
        """
        return _schema.has_schema(self._engine)

    def is_schema_compatible(self) -> bool:
        """
        Have all schema update been applied?
        """
        _LOG.info(
            "software.version",
            postgis=_schema.get_postgis_versions(self._engine),
            explorer=EXPLORER_VERSION,
        )
        return _schema.is_compatible_schema(self._engine)

    def init(self):
        """
        Initialise any schema elements that don't exist.

        (Requires `create` permissions in the db)
        """
        needed_update = not _schema.is_compatible_schema(self._engine)

        # Add any missing schema items or patches.
        _schema.create_schema(self._engine)
        refresh_also = _schema.update_schema(self._engine)

        if needed_update or refresh_also:
            _refresh_data(refresh_also, store=self)

    @classmethod
    def create(cls, index: Index, log=_LOG) -> "SummaryStore":
        return cls(index, Summariser(_utils.alchemy_engine(index)), log=log)

    def close(self):
        """Close any pooled/open connections. Necessary before forking."""
        self.index.close()
        self._engine.dispose()

    def refresh_all_products(
        self,
        refresh_older_than: timedelta = _DEFAULT_REFRESH_OLDER_THAN,
        force_dataset_extent_recompute=False,
    ):
        for product in self.all_dataset_types():
            self.refresh_product(
                product,
                refresh_older_than=refresh_older_than,
                force_dataset_extent_recompute=force_dataset_extent_recompute,
            )
        self.refresh_stats()

    def refresh_product(
        self,
        product: DatasetType,
        refresh_older_than: timedelta = _DEFAULT_REFRESH_OLDER_THAN,
        dataset_sample_size: int = 1000,
        force_dataset_extent_recompute=False,
    ) -> Optional[int]:
        """
        Update Explorer's computed extents for the given product, and record any new
        datasets into the spatial table.
        """
        our_product = self.get_product_summary(product.name)

        if (
            not force_dataset_extent_recompute
            and our_product is not None
            and our_product.last_refresh_age < refresh_older_than
        ):
            _LOG.debug(
                "init.product.skip.too_recent",
                product_name=product.name,
                age=str(our_product.last_refresh_age),
                refresh_older_than=refresh_older_than,
            )
            return None

        _LOG.info("init.product", product_name=product.name)
        change_count = _extents.refresh_product(
            self.index,
            product,
            recompute_all_extents=force_dataset_extent_recompute,
        )
        earliest, latest, total_count = self._engine.execute(
            select(
                (
                    func.min(DATASET_SPATIAL.c.center_time),
                    func.max(DATASET_SPATIAL.c.center_time),
                    func.count(),
                )
            ).where(DATASET_SPATIAL.c.dataset_type_ref == product.id)
        ).fetchone()

        source_products = []
        derived_products = []
        fixed_metadata = {}
        if total_count:
            sample_percentage = min(dataset_sample_size / total_count, 1) * 100.0
            source_products = self._get_linked_products(
                product, kind="source", sample_percentage=sample_percentage
            )
            derived_products = self._get_linked_products(
                product, kind="derived", sample_percentage=sample_percentage
            )
            fixed_metadata = self._find_product_fixed_metadata(
                product, sample_percentage=sample_percentage
            )

        self._set_product_extent(
            ProductSummary(
                product.name,
                total_count,
                earliest,
                latest,
                source_products=source_products,
                derived_products=derived_products,
                fixed_metadata=fixed_metadata,
            )
        )

        self._refresh_product_regions(product)
        _LOG.info("init.regions.done", product_name=product.name)
        return change_count

    def _refresh_product_regions(self, dataset_type: DatasetType) -> int:
        log = _LOG.bind(product_name=dataset_type.name)
        log.info("refresh.regions.start")
        changed_rows = self._engine.execute(
            """
        with srid_groups as (
             select cubedash.dataset_spatial.dataset_type_ref                         as dataset_type_ref,
                     cubedash.dataset_spatial.region_code                             as region_code,
                     ST_Transform(ST_Union(cubedash.dataset_spatial.footprint), 4326) as footprint,
                     count(*)                                                         as count
              from cubedash.dataset_spatial
              where cubedash.dataset_spatial.dataset_type_ref = %s
                    and
                    st_isvalid(cubedash.dataset_spatial.footprint)
              group by cubedash.dataset_spatial.dataset_type_ref,
                       cubedash.dataset_spatial.region_code,
                       st_srid(cubedash.dataset_spatial.footprint)
        )
        insert into cubedash.region (dataset_type_ref, region_code, footprint, count)
            select srid_groups.dataset_type_ref,
                   coalesce(srid_groups.region_code, '')                          as region_code,
                   ST_SimplifyPreserveTopology(
                           ST_Union(ST_Buffer(srid_groups.footprint, 0)), 0.0001) as footprint,
                   sum(srid_groups.count)                                         as count
            from srid_groups
            group by srid_groups.dataset_type_ref, srid_groups.region_code
        on conflict (dataset_type_ref, region_code)
            do update set count           = excluded.count,
                          generation_time = now(),
                          footprint       = excluded.footprint

            """,
            dataset_type.id,
        ).rowcount

        log.info("refresh.regions.end", changed_regions=changed_rows)
        return changed_rows

    def refresh_stats(self, concurrently=False):
        """
        Refresh general statistics tables that cover all products.

        This is ideally done once after all needed products have been refreshed.
        """
        refresh_supporting_views(self._engine, concurrently=concurrently)

    def _find_product_fixed_metadata(
        self, product: DatasetType, sample_percentage=0.05
    ) -> Dict[str, any]:
        """
        Find metadata fields that have an identical value in every dataset of the product.

        This is expensive, so only the given percentage of datasets will be sampled (but
        feel free to sample 100%!)

        """
        if not 0.0 < sample_percentage <= 100.0:
            raise ValueError(
                f"Sample percentage out of range 0>s>=100. Got {sample_percentage!r}"
            )

        # Get a single dataset, then we'll compare the rest against its values.
        first_dataset_fields = self.index.datasets.search_eager(
            product=product.name, limit=1
        )[0].metadata.fields

        SIMPLE_FIELD_TYPES = {
            "string": str,
            "numeric": (float, int),
            "double": (float, int),
            "integer": int,
            "datetime": datetime,
        }

        candidate_fields: List[Tuple[str, PgDocField]] = [
            (name, field)
            for name, field in _utils.get_mutable_dataset_search_fields(
                self.index, product.metadata_type
            ).items()
            if field.type_name in SIMPLE_FIELD_TYPES and name in first_dataset_fields
        ]

        if sample_percentage < 100:
            dataset_table = ODC_DATASET.tablesample(
                func.system(float(sample_percentage))
            ).alias("sampled_dataset")
            # Replace the table with our sampled one.
            for _, field in candidate_fields:
                if field.alchemy_column.table == ODC_DATASET:
                    field.alchemy_column = dataset_table.c[field.alchemy_column.name]

        else:
            dataset_table = ODC_DATASET

        # Give a friendlier error message when a product doesn't match the dataset.
        for name, field in candidate_fields:
            sample_value = first_dataset_fields[name]
            expected_types = SIMPLE_FIELD_TYPES[field.type_name]
            # noinspection PyTypeHints
            if sample_value is not None and not isinstance(
                sample_value, expected_types
            ):
                raise ValueError(
                    f"Product {product.name} field {name!r} is "
                    f"claimed to be type {expected_types}, but dataset has value {sample_value!r}"
                )

        _LOG.info(
            "product.fixed_metadata_search",
            product=product.name,
            sample_percentage=round(sample_percentage, 2),
        )
        result: List[RowProxy] = self._engine.execute(
            select(
                [
                    (
                        func.every(
                            field.alchemy_expression == first_dataset_fields[field_name]
                        )
                    ).label(field_name)
                    for field_name, field in candidate_fields
                ]
            )
            .select_from(dataset_table)
            .where(dataset_table.c.dataset_type_ref == product.id)
            .where(dataset_table.c.archived == None)
        ).fetchall()
        assert len(result) == 1

        fixed_fields = {
            key: first_dataset_fields[key]
            for key, is_fixed in result[0].items()
            if is_fixed
        }
        _LOG.info(
            "product.fixed_metadata_search.done",
            product=product.name,
            sample_percentage=round(sample_percentage, 2),
            searched_field_count=len(result[0]),
            found_field_count=len(fixed_fields),
        )
        return fixed_fields

    def _get_linked_products(
        self, product: DatasetType, kind="source", sample_percentage=0.05
    ) -> List[str]:
        """
        Find products with upstream or downstream datasets from this product.

        It only samples a percentage of this product's datasets, due to slow speed. (But 1 dataset
        would be enough for most products)
        """
        if kind not in ("source", "derived"):
            raise ValueError(f"Unexpected kind of link: {kind!r}")
        if not 0.0 < sample_percentage <= 100.0:
            raise ValueError(
                f"Sample percentage out of range 0>s>=100. Got {sample_percentage!r}"
            )

        from_ref, to_ref = "source_dataset_ref", "dataset_ref"
        if kind == "derived":
            to_ref, from_ref = from_ref, to_ref

        # Avoid tablesample (full table scan) when we're getting all of the product anyway.
        sample_sql = ""
        if sample_percentage < 100:
            sample_sql = "tablesample system (%(sample_percentage)s)"

        (linked_product_names,) = self._engine.execute(
            f"""
            with datasets as (
                select id from agdc.dataset {sample_sql}
                where dataset_type_ref=%(product_id)s
                and archived is null
            ),
            linked_datasets as (
                select distinct {from_ref} as linked_dataset_ref
                from agdc.dataset_source
                inner join datasets d on d.id = {to_ref}
            ),
            linked_products as (
                select distinct dataset_type_ref
                from agdc.dataset
                inner join linked_datasets on id = linked_dataset_ref
                where archived is null
            )
            select array_agg(name order by name)
            from agdc.dataset_type
            inner join linked_products sp on id = dataset_type_ref;
        """,
            product_id=product.id,
            sample_percentage=sample_percentage,
        ).fetchone()

        _LOG.info(
            f"product.links.{kind}",
            product=product.name,
            linked=linked_product_names,
            sample_percentage=round(sample_percentage, 2),
        )
        return list(linked_product_names or [])

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
        force_refresh: Optional[bool] = False,
    ) -> Optional[TimePeriodOverview]:
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
        period = "all"
        if year:
            period = "year"
        if month:
            period = "month"
        if day:
            period = "day"

        return date(year or 1900, month or 1, day or 1), period

    # These are cached to avoid repeated unnecessary DB queries.
    @ttl_cache(ttl=DEFAULT_TTL)
    def all_dataset_types(self) -> Iterable[DatasetType]:
        return tuple(self.index.products.get_all())

    @ttl_cache(ttl=DEFAULT_TTL)
    def all_metadata_types(self) -> Iterable[DatasetType]:
        return tuple(self.index.metadata_types.get_all())

    @ttl_cache(ttl=DEFAULT_TTL)
    def get_dataset_type(self, name) -> DatasetType:
        for d in self.all_dataset_types():
            if d.name == name:
                return d
        raise KeyError(f"Unknown dataset type {name!r}")

    @ttl_cache(ttl=DEFAULT_TTL)
    def _dataset_type_by_id(self, id_) -> DatasetType:
        for d in self.all_dataset_types():
            if d.id == id_:
                return d
        raise KeyError(f"Unknown dataset type id {id_!r}")

    @ttl_cache(ttl=DEFAULT_TTL)
    def _product(self, name: str) -> ProductSummary:
        row = self._engine.execute(
            select(
                [
                    PRODUCT.c.dataset_count,
                    PRODUCT.c.time_earliest,
                    PRODUCT.c.time_latest,
                    (func.now() - PRODUCT.c.last_refresh).label("last_refresh_age"),
                    PRODUCT.c.id.label("id_"),
                    PRODUCT.c.source_product_refs,
                    PRODUCT.c.derived_product_refs,
                    PRODUCT.c.fixed_metadata,
                ]
            ).where(PRODUCT.c.name == name)
        ).fetchone()
        if not row:
            raise ValueError(f"Unknown product {name!r} (initialised?)")

        row = dict(row)
        source_products = [
            self._dataset_type_by_id(id_).name for id_ in row.pop("source_product_refs")
        ]
        derived_products = [
            self._dataset_type_by_id(id_).name
            for id_ in row.pop("derived_product_refs")
        ]

        return ProductSummary(
            name=name,
            source_products=source_products,
            derived_products=derived_products,
            **row,
        )

    @ttl_cache(ttl=DEFAULT_TTL)
    def product_location_samples(self, name: str) -> List[ProductLocationSample]:
        """
        Sample some dataset locations for the given product, and return
        the common location.

        Returns one row for each uri scheme found (http, file etc).
        """
        # Sample 100 dataset uris
        uri_samples = sorted(
            [
                uri
                for [uri] in self.index.datasets.search_returning(
                    ("uri",), product=name, limit=100
                )
            ]
        )

        def uri_scheme(uri: str):
            return uri.split(":", 1)[0]

        location_schemes = []
        for scheme, uris in groupby(uri_samples, uri_scheme):
            uris = list(uris)

            # Use the first, last and middle as examples
            # (they're sorted, so this shows diversity)
            example_uris = {uris[0], uris[-1], uris[int(len(uris) / 2)]}
            #              ⮤ we use a set for when len < 3

            location_schemes.append(
                ProductLocationSample(
                    scheme, os.path.commonpath(uris), sorted(example_uris)
                )
            )

        return location_schemes

    def get_quality_stats(self) -> Iterable[Dict]:
        stats = self._engine.execute(select([SPATIAL_QUALITY_STATS]))
        for row in stats:
            d = dict(row)
            d["product"] = self._dataset_type_by_id(row["dataset_type_ref"])
            d["avg_footprint_bytes"] = (
                row["footprint_size"] / row["count"] if row["footprint_size"] else 0
            )
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

    def _set_product_extent(self, product: ProductSummary) -> int:
        source_product_ids = [
            self.index.products.get_by_name(name).id for name in product.source_products
        ]
        derived_product_ids = [
            self.index.products.get_by_name(name).id
            for name in product.derived_products
        ]
        fields = dict(
            dataset_count=product.dataset_count,
            time_earliest=product.time_earliest,
            time_latest=product.time_latest,
            source_product_refs=source_product_ids,
            derived_product_refs=derived_product_ids,
            fixed_metadata=product.fixed_metadata,
            # Deliberately do all age calculations with the DB clock rather than local.
            last_refresh=func.now(),
        )

        # Dear future reader. This section used to use an 'UPSERT' statement (as in,
        # insert, on_conflict...) and while this works, it triggers the sequence
        # `product_id_seq` to increment as part of the check for insertion. This
        # is bad because there's only 32 k values in the sequence and we have run out
        # a couple of times! So, It appears that this update-else-insert must be done
        # in two transactions...
        row = self._engine.execute(
            select([PRODUCT.c.id]).where(PRODUCT.c.name == product.name)
        ).fetchone()

        if row:
            # Product already exists, so update it
            self._engine.execute(
                PRODUCT.update().where(PRODUCT.c.id == row[0]).values(fields)
            )
        else:
            # Product doesn't exist, so insert it
            row = self._engine.execute(
                postgres.insert(PRODUCT).values(**fields, name=product.name)
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

    def _add_fields_to_query(
        self,
        query: Select,
        product_names: Optional[List[str]] = None,
        time: Optional[Tuple[datetime, datetime]] = None,
        bbox: Tuple[float, float, float, float] = None,
        dataset_ids: Sequence[UUID] = None,
        require_geometry=True,
    ) -> Select:
        # If they specify IDs, all other search parameters are ignored.
        # (from Stac API spec)
        if dataset_ids is not None:
            query = query.where(DATASET_SPATIAL.c.id.in_(dataset_ids))
        else:
            if time:
                query = query.where(
                    func.tstzrange(
                        _utils.default_utc(time[0]),
                        _utils.default_utc(time[1]),
                        "[]",
                        type_=TSTZRANGE,
                    ).contains(DATASET_SPATIAL.c.center_time)
                )

            if bbox:
                query = query.where(
                    func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326).intersects(
                        func.ST_MakeEnvelope(*bbox)
                    )
                )

            if product_names:
                if len(product_names) == 1:
                    query = query.where(
                        DATASET_SPATIAL.c.dataset_type_ref
                        == select([ODC_DATASET_TYPE.c.id]).where(
                            ODC_DATASET_TYPE.c.name == product_names[0]
                        )
                    )
                else:
                    query = query.where(
                        DATASET_SPATIAL.c.dataset_type_ref.in_(
                            select([ODC_DATASET_TYPE.c.id]).where(
                                ODC_DATASET_TYPE.c.name.in_(product_names)
                            )
                        )
                    )

        if require_geometry:
            query = query.where(DATASET_SPATIAL.c.footprint != None)

        return query

    @ttl_cache(ttl=DEFAULT_TTL)
    def get_arrivals(
        self, period_length: timedelta
    ) -> List[Tuple[date, List[ProductArrival]]]:
        """
        Get a list of products with newly added datasets for the last few days.
        """
        latest_arrival_date: datetime = self._engine.execute(
            "select max(added) from agdc.dataset;"
        ).scalar()
        if latest_arrival_date is None:
            return []

        datasets_since_date = (latest_arrival_date - period_length).date()

        current_day = None
        products = []
        out_groups = []
        for day, product_name, count, dataset_ids in self._engine.execute(
            """
            select
               date_trunc('day', added) as arrival_date,
               (select name from agdc.dataset_type where id = d.dataset_type_ref) product_name,
               count(*),
               (array_agg(id))[0:3]
            from agdc.dataset d
            where d.added > %(datasets_since)s
            group by arrival_date, product_name
            order by arrival_date desc, product_name;
        """,
            datasets_since=datasets_since_date,
        ):
            if current_day is None:
                current_day = day

            if day != current_day:
                out_groups.append((current_day, products))
                products = []
                current_day = day
            products.append(ProductArrival(product_name, day, count, dataset_ids))

        if products:
            out_groups.append((products[0].day, products))

        return out_groups

    def get_count(
        self,
        product_names: Optional[List[str]] = None,
        time: Optional[Tuple[datetime, datetime]] = None,
        bbox: Tuple[float, float, float, float] = None,
        dataset_ids: Sequence[UUID] = None,
        require_geometry=True,
    ) -> int:
        """
        Do the most simple select query to get the count of matching datasets.
        """
        query: Select = select([func.count()]).select_from(DATASET_SPATIAL)

        query = self._add_fields_to_query(
            query,
            product_names=product_names,
            time=time,
            bbox=bbox,
            dataset_ids=dataset_ids,
            require_geometry=require_geometry,
        )

        result = self._engine.execute(query).fetchall()

        if len(result) != 0:
            return result[0][0]
        else:
            return 0

    def search_items(
        self,
        *,
        product_names: Optional[List[str]] = None,
        time: Optional[Tuple[datetime, datetime]] = None,
        bbox: Tuple[float, float, float, float] = None,
        limit: int = 500,
        offset: int = 0,
        full_dataset: bool = False,
        dataset_ids: Sequence[UUID] = None,
        require_geometry=True,
        ordered=True,
    ) -> Generator[DatasetItem, None, None]:
        """
        Search datasets using Cubedash's spatial table

        Returned as DatasetItem records, with optional embedded full Datasets
        (if full_dataset==True)

        Returned results are always sorted by (center_time, id)
        """
        geom = func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326)

        columns = [
            geom.label("geometry"),
            func.Box2D(geom).label("bbox"),
            # TODO: dataset label?
            DATASET_SPATIAL.c.region_code.label("region_code"),
            DATASET_SPATIAL.c.creation_time,
            DATASET_SPATIAL.c.center_time,
        ]

        # If fetching the whole dataset, we need to join the ODC dataset table.
        if full_dataset:
            query: Select = select(
                (*columns, *_utils.DATASET_SELECT_FIELDS)
            ).select_from(
                DATASET_SPATIAL.join(
                    ODC_DATASET, onclause=ODC_DATASET.c.id == DATASET_SPATIAL.c.id
                )
            )
        # Otherwise query purely from the spatial table.
        else:
            query: Select = select(
                (*columns, DATASET_SPATIAL.c.id, DATASET_SPATIAL.c.dataset_type_ref)
            ).select_from(DATASET_SPATIAL)

        # Add all the filters
        query = self._add_fields_to_query(
            query,
            product_names=product_names,
            time=time,
            bbox=bbox,
            dataset_ids=dataset_ids,
            require_geometry=require_geometry,
        )

        # Maybe sort
        if ordered:
            query = query.order_by(DATASET_SPATIAL.c.center_time, DATASET_SPATIAL.c.id)

        query = query.limit(limit).offset(
            # TODO: Offset/limit isn't particularly efficient for paging...
            offset
        )

        for r in self._engine.execute(query):
            yield DatasetItem(
                dataset_id=r.id,
                bbox=_box2d_to_bbox(r.bbox) if r.bbox else None,
                product_name=self.index.products.get(r.dataset_type_ref).name,
                geometry=_get_shape(r.geometry, self._get_srid_name(r.geometry.srid)),
                region_code=r.region_code,
                creation_time=r.creation_time,
                center_time=r.center_time,
                odc_dataset=(
                    _utils.make_dataset_from_select_fields(self.index, r)
                    if full_dataset
                    else None
                ),
            )

    def get_or_update(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        force_refresh: Optional[bool] = False,
    ) -> TimePeriodOverview:
        """
        Get a cached summary if exists, otherwise generate one

        Note that generating one can be *extremely* slow.
        """
        if force_refresh:
            summary = self.update(
                product_name,
                year,
                month,
                day,
                generate_missing_children=True,
                force_refresh=True,
            )
        else:
            summary = self.get(product_name, year, month, day)
        if not summary:
            summary = self.update(product_name, year, month, day)
        return summary

    def update(
        self,
        product_name: Optional[str],
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        generate_missing_children: Optional[bool] = True,
        force_refresh: Optional[bool] = False,
    ) -> TimePeriodOverview:
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
                get_child(product_name, year, month_, None, force_refresh=force_refresh)
                for month_ in range(1, 13)
            )
        elif product_name:
            if product.dataset_count > 0:
                years = range(product.time_earliest.year, product.time_latest.year + 1)
            else:
                years = []
            summary = TimePeriodOverview.add_periods(
                get_child(product_name, year_, None, None, force_refresh=force_refresh)
                for year_ in years
            )
        else:
            summary = TimePeriodOverview.add_periods(
                get_child(product.name, None, None, None, force_refresh=force_refresh)
                for product in self.all_dataset_types()
            )

        self._do_put(product_name, year, month, day, summary)

        for listener in self._update_listeners:
            listener(product_name, year, month, day, summary)
        return summary

    @ttl_cache(ttl=DEFAULT_TTL)
    def _get_srid_name(self, srid: int):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        return get_srid_name(self._engine, srid)

    def _do_put(self, product_name, year, month, day, summary):
        log = _LOG.bind(
            product_name=product_name,
            time=(year, month, day),
            summary_count=summary.dataset_count,
        )
        # Don't bother storing empty periods that are outside of the existing range.
        # This doesn't have to be exact (note that someone may update in parallel too).
        if summary.dataset_count == 0 and (year or month):
            product = self.get_product_summary(product_name)
            if (not product) or (not product.time_latest):
                log.debug("product.empty")
                return

            timezone = tz.gettz(self._summariser.grouping_time_zone)
            if (
                datetime(year, month or 12, day or 28, tzinfo=timezone)
                < product.time_earliest
            ):
                log.debug("product.skip.before_range")
                return
            if (
                datetime(year, month or 1, day or 1, tzinfo=timezone)
                > product.time_latest
            ):
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
                product.name
                for product in all_products
                if self.has(product.name, None, None, None)
            )
        )
        return existing_products

    def get_last_updated(self) -> Optional[datetime]:
        """Time of last update, if known"""
        return None

    def find_datasets_for_region(
        self,
        product_name: str,
        region_code: str,
        year: int,
        month: int,
        day: int,
        limit: int,
    ) -> Iterable[Dataset]:

        time_range = _utils.as_time_range(
            year, month, day, tzinfo=self.grouping_timezone
        )
        return _extents.datasets_by_region(
            self._engine, self.index, product_name, region_code, time_range, limit
        )

    @ttl_cache(ttl=DEFAULT_TTL)
    def _region_summaries(self, product_name: str) -> Dict[str, RegionSummary]:
        dt = self.get_dataset_type(product_name)
        return {
            code: RegionSummary(
                product_name=product_name,
                region_code=code,
                count=count,
                generation_time=generation_time,
                footprint_wgs84=to_shape(geom),
            )
            for code, count, generation_time, geom in self._engine.execute(
                select(
                    [
                        REGION.c.region_code,
                        REGION.c.count,
                        REGION.c.generation_time,
                        REGION.c.footprint,
                    ]
                )
                .where(REGION.c.dataset_type_ref == dt.id)
                .order_by(REGION.c.region_code)
            )
            if geom is not None
        }

    def get_product_region_info(self, product_name: str) -> RegionInfo:
        return RegionInfo.for_product(
            dataset_type=self.get_dataset_type(product_name),
            known_regions=self._region_summaries(product_name),
        )

    def get_dataset_footprint_region(self, dataset_id):
        """
        Get the recorded WGS84 footprint and region code for a given dataset.

        Note that these will be None if the product has not been summarised.
        """
        rows = self._engine.execute(
            select(
                [
                    func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326).label(
                        "footprint"
                    ),
                    DATASET_SPATIAL.c.region_code,
                ]
            ).where(DATASET_SPATIAL.c.id == dataset_id)
        ).fetchall()
        if not rows:
            return None, None
        row = rows[0]

        footprint = row.footprint
        return (
            to_shape(footprint) if footprint is not None else None,
            row.region_code,
        )


def _refresh_data(please_refresh: Set[PleaseRefresh], store: SummaryStore):
    """
    Refresh product information after a schema update, plus the given kind of data.
    """
    recompute_dataset_extents = PleaseRefresh.DATASET_EXTENTS in please_refresh

    for dt in store.all_dataset_types():
        _LOG.info("data.refreshing_extents", product=dt.name)
        # Skip product if it's never been summarised at all.
        if store.get_product_summary(dt.name) is None:
            continue

        store.refresh_product(
            dt,
            refresh_older_than=timedelta(minutes=-1),
            force_dataset_extent_recompute=recompute_dataset_extents,
        )
    _LOG.info("data.refreshing_extents.complete")


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
    day_values, day_counts = _counter_key_vals(summary.timeline_dataset_counts)
    region_values, region_counts = _counter_key_vals(summary.region_dataset_counts)

    begin, end = summary.time_range if summary.time_range else (None, None)

    if summary.footprint_geometry and summary.footprint_srid is None:
        raise ValueError("Geometry without srid", summary)

    return dict(
        dataset_count=summary.dataset_count,
        timeline_dataset_start_days=day_values,
        timeline_dataset_counts=day_counts,
        # TODO: SQLAlchemy needs a bit of type help for some reason. Possible PgGridCell bug?
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


def _counter_key_vals(counts: Counter, null_sort_key="ø") -> Tuple[Tuple, Tuple]:
    """
    Split counter into a keys sequence and a values sequence.

    (Both sorted by key)

    >>> tuple(_counter_key_vals(Counter(['a', 'a', 'b'])))
    (('a', 'b'), (2, 1))
    >>> tuple(_counter_key_vals(Counter(['a'])))
    (('a',), (1,))
    >>> tuple(_counter_key_vals(Counter(['a', None])))
    (('a', None), (1, 1))
    >>> # Important! zip(*) doesn't do this.
    >>> tuple(_counter_key_vals(Counter()))
    ((), ())
    """
    items = sorted(
        counts.items(),
        # Swap nulls if needed.
        key=lambda t: (null_sort_key, t[1]) if t[0] is None else t,
    )
    return tuple(k for k, v in items), tuple(v for k, v in items)


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
            "creation_time": _utils.dataset_created(dataset),
        },
    }


_BOX2D_PATTERN = re.compile(
    r"BOX\(([-0-9.e]+)\s+([-0-9.e]+)\s*,\s*([-0-9.e]+)\s+([-0-9.e]+)\)"
)


def _box2d_to_bbox(pg_box2d: str) -> Tuple[float, float, float, float]:
    """
    Parse Postgis's box2d to a geojson/stac bbox tuple.

    >>> _box2d_to_bbox(
    ...     "BOX(134.806923200497 -17.7694714883835,135.769692610214 -16.8412669214876)"
    ... )
    (134.806923200497, -17.7694714883835, 135.769692610214, -16.8412669214876)
    >>> # Scientific notation in numbers is sometimes given
    >>> _box2d_to_bbox(
    ...     "BOX(35.6948526641442 -0.992278901187827,36.3518945675102 -9.03173177994956e-06)"
    ... )
    (35.6948526641442, -0.992278901187827, 36.3518945675102, -9.03173177994956e-06)
    """
    m = _BOX2D_PATTERN.match(pg_box2d)
    if m is None:
        raise RuntimeError(f"Unexpected postgis box syntax {pg_box2d!r}")

    # We know there's exactly four groups, but type checker doesn't...
    # noinspection PyTypeChecker
    return tuple(float(m) for m in m.groups())


def _get_shape(geometry: WKBElement, crs) -> Optional[Geometry]:
    """
    Our shapes are valid in the db, but can become invalid on
    reprojection. We buffer if needed.

    Eg invalid. 32baf68c-7d91-4e13-8860-206ac69147b0

    (the tests reproduce this error.... but it may be machine/environment dependent?)
    """
    if geometry is None:
        return None

    shape = Geometry(to_shape(geometry), crs).to_crs("EPSG:4326", wrapdateline=True)

    if not shape.is_valid:
        newshape = shape.buffer(0)
        assert math.isclose(
            shape.area, newshape.area, abs_tol=0.0001
        ), f"{shape.area} != {newshape.area}"
        shape = newshape
    return shape


if __name__ == "__main__":
    # For debugging store commands...
    with Datacube() as dc:
        from pprint import pprint

        store = SummaryStore.create(dc.index)
        pprint(
            store._find_product_fixed_metadata(
                dc.index.products.get_by_name("ls8_nbar_scene"), sample_percentage=50
            )
        )
