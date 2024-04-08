import math
import re
from collections import Counter, defaultdict
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum, auto
from itertools import groupby
from typing import (
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from uuid import UUID

import dateutil.parser
import pytz
import structlog
from cachetools.func import lru_cache, ttl_cache
from dateutil import tz
from geoalchemy2 import WKBElement
from geoalchemy2 import shape as geo_shape
from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry.base import BaseGeometry
from sqlalchemy import DDL, String, and_, exists, func, literal, or_, select, union_all
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select

try:
    from cubedash._version import version as explorer_version
except ModuleNotFoundError:
    explorer_version = "ci-test-pipeline"
from datacube import Datacube
from datacube.drivers.postgres._fields import PgDocField
from datacube.index import Index
from datacube.model import Dataset, DatasetType, Range
from datacube.utils.geometry import Geometry

from cubedash import _utils
from cubedash._utils import ODC_DATASET, ODC_DATASET_LOCATION, ODC_DATASET_TYPE
from cubedash.summary import RegionInfo, TimePeriodOverview, _extents, _schema
from cubedash.summary._extents import (
    ProductArrival,
    RegionSummary,
    dataset_changed_expression,
    datetime_expression,
)
from cubedash.summary._schema import (
    DATASET_SPATIAL,
    FOOTPRINT_SRID_EXPRESSION,
    PRODUCT,
    REGION,
    SPATIAL_QUALITY_STATS,
    TIME_OVERVIEW,
    PleaseRefresh,
    get_srid_name,
    refresh_supporting_views,
)
from cubedash.summary._summarise import DEFAULT_TIMEZONE, Summariser

DEFAULT_TTL = 90

_DEFAULT_REFRESH_OLDER_THAN = timedelta(hours=23)

_LOG = structlog.get_logger()

# The default grouping epsg code to use on init of a new Explorer schema.
#
# We'll use a global equal area.
DEFAULT_EPSG = 6933

default_timezone = pytz.timezone(DEFAULT_TIMEZONE)


class ItemSort(Enum):
    # The fastest, but paging is unusable.
    UNSORTED = auto()
    # Sort by time then dataset id. Stable for paging.
    DEFAULT_SORT = auto()
    # Sort by time indexed into ODC, most recent first.
    # (this doesn't work very efficiently with other filters, like bbox.)
    RECENTLY_ADDED = auto()


class GenerateResult(Enum):
    """What happened in a product refresh task?"""

    # Product was newly generated (or force-refreshed to recreate everything).
    CREATED = 2
    # Updated the existing summaries (for months that changed)
    UPDATED = 3
    # No new changes found.
    NO_CHANGES = 1
    # Exception was thrown
    ERROR = 4
    # A unsupported product (eg. Unsupported CRS)
    UNSUPPORTED = 5


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

    # The db-server-local time when this product record+extent was refreshed.
    last_refresh_time: datetime

    # The `last_refresh_time` last time when summary generation was last fully completed.
    # (To find changes, we'll scan any datasets newer than this date)
    last_successful_summary_time: datetime = None

    # Not recommended for use by users, as ids are local and internal.
    # The 'name' is typically used as an identifier, and with ODC itself.
    id_: Optional[int] = None

    def iter_months(
        self, grouping_timezone=default_timezone
    ) -> Generator[date, None, None]:
        """
        Iterate through all months in its time range.
        """
        if self.dataset_count == 0:
            return

        start = (
            self.time_earliest.astimezone(grouping_timezone)
            if self.time_earliest
            else self.time_earliest
        )
        end = (
            self.time_latest.astimezone(grouping_timezone)
            if self.time_latest
            else self.time_latest
        )
        if start > end:
            raise ValueError(f"Start date must precede end date ({start} < {end})")

        year = start.year
        month = start.month
        while True:
            yield date(year, month, 1)

            month += 1
            if month == 13:
                month = 1
                year += 1

            if (year, month) > (end.year, end.month):
                return


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

        # How much extra time to include in incremental update scans?
        #    The incremental-updater searches for any datasets with a newer change-timestamp than
        #    its last successul run. But some earlier-timestamped datasets may not have been
        #    present last run if they were added in a concurrent, open transaction. And we don't
        #    want to miss them! So we give a buffer assuming no transaction was open longer than
        #    this buffer. (It doesn't matter at all if we repeat datasets).
        #
        #    This is not solution of perfection. But ODC's indexing does happen with quick,
        #    auto-committing transactions, so they're unlikely to actually be open for more
        #    than a few milliseconds. Fifteen minutes feels very generous.
        #
        #    (You can judge if this assumption has failed by comparing our dataset_spatial
        #     count(*) to ODC's dataset count(*) for the same product. They should match
        #     for active datasets.)
        #
        #    tldr: "15 minutes == max expected transaction age of indexer"
        self.dataset_overlap_carefulness = timedelta(minutes=15)

    def add_change_listener(self, listener):
        self._update_listeners.append(listener)

    def is_initialised(self) -> bool:
        """
        Do our DB schemas exist?
        """
        return _schema.has_schema(self._engine)

    def is_schema_compatible(self, for_writing_operations_too=False) -> bool:
        """
        Have all schema update been applied?
        """
        _LOG.debug(
            "software.version",
            postgis=_schema.get_postgis_versions(self._engine),
            explorer=explorer_version,
        )
        if for_writing_operations_too:
            return _schema.is_compatible_generate_schema(self._engine)
        else:
            return _schema.is_compatible_schema(self._engine)

    def init(self, grouping_epsg_code: int = None):
        """
        Initialise any schema elements that don't exist.

        Takes an epsg_code, of the CRS used internally for summaries.

        (Requires `create` permissions in the db)
        """

        # Add any missing schema items or patches.
        _schema.create_schema(
            self._engine, epsg_code=grouping_epsg_code or DEFAULT_EPSG
        )

        # If they specified an epsg code, make sure the existing schema uses it.
        if grouping_epsg_code:
            crs_used_by_schema = self.grouping_crs
            if crs_used_by_schema != f"EPSG:{grouping_epsg_code}":
                raise RuntimeError(
                    f"""
                Tried to initialise with EPSG:{grouping_epsg_code!r},
                but the schema is already using {crs_used_by_schema}.

                To change the CRS, you need to recreate Explorer's schema.

                Eg.

                    # Drop schema
                    cubedash-gen --drop

                    # Create schema with new epsg, and summarise all products again.
                    cubedash-gen --init --epsg {grouping_epsg_code} --all

                (Warning: Resummarising all of your products may take a long time!)
                """
                )
        refresh_also = _schema.update_schema(self._engine)

        if refresh_also:
            _refresh_data(refresh_also, store=self)

    @classmethod
    def create(
        cls, index: Index, log=_LOG, grouping_time_zone=DEFAULT_TIMEZONE
    ) -> "SummaryStore":
        return cls(
            index,
            Summariser(
                _utils.alchemy_engine(index), grouping_time_zone=grouping_time_zone
            ),
            log=log,
        )

    @property
    def grouping_crs(self):
        """
        Get the crs name used for grouping summaries.

        (the value that was set on ``init()`` of the schema)
        """
        return self._get_srid_name(
            self._engine.execute(select([FOOTPRINT_SRID_EXPRESSION])).scalar()
        )

    def close(self):
        """Close any pooled/open connections. Necessary before forking."""
        self.index.close()
        self._engine.dispose()

    def refresh_all_product_extents(
        self,
    ):
        for product in self.all_dataset_types():
            self.refresh_product_extent(
                product.name,
            )
        self.refresh_stats()

    def find_most_recent_change(self, product_name: str):
        """
        Find the database-local time of the last dataset that changed for this product.
        """
        dataset_type = self.get_dataset_type(product_name)

        return self._engine.execute(
            select(
                [
                    func.max(dataset_changed_expression()),
                ]
            ).where(ODC_DATASET.c.dataset_type_ref == dataset_type.id)
        ).scalar()

    def find_months_needing_update(
        self,
        product_name: str,
        only_those_newer_than: datetime,
    ) -> Iterable[Tuple[date, int]]:
        """
        What months have had dataset changes since they were last generated?
        """
        dataset_type = self.get_dataset_type(product_name)

        # Find the most-recently updated datasets and group them by month.
        return sorted(
            (month.date(), count)
            for month, count in self._engine.execute(
                select(
                    [
                        func.date_trunc(
                            "month", datetime_expression(dataset_type.metadata_type)
                        ).label("month"),
                        func.count(),
                    ]
                )
                .where(ODC_DATASET.c.dataset_type_ref == dataset_type.id)
                .where(dataset_changed_expression() > only_those_newer_than)
                .group_by("month")
                .order_by("month")
            )
        )

    def find_years_needing_update(self, product_name: str) -> List[int]:
        """
        Find any years that need to be generated.

        Either:
           1) They don't exist yet, or
           2) They existed before and has been deleted or archived, or
           3) They have month-records that are newer than our year-record.
        """
        updated_months = TIME_OVERVIEW.alias("updated_months")
        years = TIME_OVERVIEW.alias("years_needing_update")
        product = self.get_product_summary(product_name)

        # Years that have already been summarised
        summarised_years = {
            r[0].year
            for r in self._engine.execute(
                select([years.c.start_day])
                .where(years.c.period_type == "year")
                .where(
                    years.c.product_ref == product.id_,
                )
            )
        }

        # Empty product? No years
        if product.dataset_count == 0:
            # check if the timeoverview needs cleanse
            if not summarised_years:
                return []
            else:
                return summarised_years

        # All years we are expected to have
        expected_years = set(
            range(
                product.time_earliest.astimezone(self.grouping_timezone).year,
                product.time_latest.astimezone(self.grouping_timezone).year + 1,
            )
        )

        missing_years = expected_years.difference(summarised_years)

        # Years who have month-records updated more recently than their own record.
        outdated_years = {
            start_day.year
            for [start_day] in self._engine.execute(
                # Select years
                select([years.c.start_day])
                .where(years.c.period_type == "year")
                .where(
                    years.c.product_ref == product.id_,
                )
                # Where there exist months that are more newly created.
                .where(
                    exists(
                        select([updated_months.c.start_day])
                        .where(updated_months.c.period_type == "month")
                        .where(
                            func.extract("year", updated_months.c.start_day)
                            == func.extract("year", years.c.start_day)
                        )
                        .where(
                            updated_months.c.product_ref == product.id_,
                        )
                        .where(
                            updated_months.c.generation_time > years.c.generation_time
                        )
                    )
                )
            )
        }
        return sorted(missing_years.union(outdated_years))

    def needs_extent_refresh(self, product_name: str) -> bool:
        """
        Does the given product have changes since the last refresh?
        """
        existing_product_summary = self.get_product_summary(product_name)
        if not existing_product_summary:
            # Never been summarised. So, yes!
            return True

        most_recent_change = self.find_most_recent_change(product_name)
        has_new_changes = most_recent_change and (
            most_recent_change > existing_product_summary.last_refresh_time
        )

        _LOG.debug(
            "product.last_extent_changes",
            product_name=product_name,
            last_refresh_time=existing_product_summary.last_refresh_time,
            most_recent_change=most_recent_change,
            has_new_changes=has_new_changes,
        )
        return has_new_changes

    def refresh_product_extent(
        self,
        product_name: str,
        dataset_sample_size: int = 1000,
        scan_for_deleted: bool = False,
        only_those_newer_than: datetime = None,
        force: bool = False,
    ) -> Tuple[int, ProductSummary]:
        """
        Update Explorer's computed extents for the given product, and record any new
        datasets into the spatial table.

        Returns the count of changed dataset extents, and the
        updated product summary.
        """
        # Server-side-timestamp of when we started scanning. We will
        # later know that any dataset newer than this timestamp may not
        # be in our summaries.
        covers_up_to = self._database_time_now()

        product = self.index.products.get_by_name(product_name)

        _LOG.info("init.product", product_name=product.name)
        change_count = _extents.refresh_spatial_extents(
            self.index,
            product,
            clean_up_deleted=scan_for_deleted,
            assume_after_date=only_those_newer_than,
        )

        existing_summary = self.get_product_summary(product_name)
        # Did nothing change at all? Just bump the refresh time.
        if change_count == 0 and existing_summary and not force:
            new_summary = copy(existing_summary)
            new_summary.last_refresh_time = covers_up_to
            self._persist_product_extent(new_summary)
            return 0, new_summary

        # if change_count or force_dataset_extent_recompute:
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
                product, sample_datasets_size=dataset_sample_size
            )

        new_summary = ProductSummary(
            product.name,
            total_count,
            earliest,
            latest,
            source_products=source_products,
            derived_products=derived_products,
            fixed_metadata=fixed_metadata,
            last_refresh_time=covers_up_to,
        )

        # TODO: This is an expensive operation. We regenerate them all every time there are changes.
        self._refresh_product_regions(product)

        self._persist_product_extent(new_summary)
        return change_count, new_summary

    def _refresh_product_regions(self, dataset_type: DatasetType) -> int:
        log = _LOG.bind(product_name=dataset_type.name, engine=str(self._engine))
        log.info("refresh.regions.start")

        log.info("refresh.regions.update.count.and.insert.new")

        # add new regions row and/or update existing regions based on dataset_spatial
        with self._engine.begin() as conn:
            result = conn.execute(
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
            returning dataset_type_ref, region_code, footprint, count

                """,
                dataset_type.id,
            )
            log.info("refresh.regions.inserted", list(result))
            changed_rows = result.rowcount
            log.info(
                "refresh.regions.update.count.and.insert.new.end",
                changed_rows=changed_rows,
            )

            # delete region rows with no related datasets in dataset_spatial table
            log.info("refresh.regions.delete.empty.regions")
            result = conn.execute(
                """
            delete from cubedash.region
            where dataset_type_ref = %s and region_code not in (
                 select cubedash.dataset_spatial.region_code
                 from cubedash.dataset_spatial
                 where cubedash.dataset_spatial.dataset_type_ref = %s
                 group by cubedash.dataset_spatial.region_code
            )
                """,
                dataset_type.id,
                dataset_type.id,
            )
            changed_rows = result.rowcount
        log.info("refresh.regions.delete.empty.regions.end")

        log.info("refresh.regions.end", changed_regions=changed_rows)
        return changed_rows

    def refresh_stats(self, concurrently=False):
        """
        Refresh general statistics tables that cover all products.

        This is ideally done once after all needed products have been refreshed.
        """
        refresh_supporting_views(self._engine, concurrently=concurrently)

    def _find_product_fixed_metadata(
        self,
        product: DatasetType,
        sample_datasets_size=1000,
    ) -> Dict[str, any]:
        """
        Find metadata fields that have an identical value in every dataset of the product.

        This is expensive, so only the given percentage of datasets will be sampled (but
        feel free to sample 100%!)

        """
        # Get a single dataset, then we'll compare the rest against its values.
        first_dataset_fields = self.index.datasets.search_eager(
            product=product.name, limit=1
        )[0].metadata.fields

        simple_field_types = {
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
            if field.type_name in simple_field_types and name in first_dataset_fields
        ]

        # Give a friendlier error message when a product doesn't match the dataset.
        for name, field in candidate_fields:
            sample_value = first_dataset_fields[name]
            expected_types = simple_field_types[field.type_name]
            # noinspection PyTypeHints
            if sample_value is not None and not isinstance(
                sample_value, expected_types
            ):
                raise ValueError(
                    f"Product {product.name} field {name!r} is "
                    f"claimed to be type {expected_types}, but dataset has value {sample_value!r}"
                )

        dataset_samples = self._engine.execute(
            select([ODC_DATASET.c.id])
            .select_from(ODC_DATASET)
            .where(ODC_DATASET.c.dataset_type_ref == product.id)
            .where(ODC_DATASET.c.archived.is_(None))
            .limit(sample_datasets_size)
            .order_by(func.random())
        ).fetchall()

        _LOG.info(
            "product.fixed_metadata_search",
            product=product.name,
            sampled_dataset_count=sample_datasets_size,
        )

        result = self._engine.execute(
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
            .select_from(ODC_DATASET)
            .where(ODC_DATASET.c.id.in_([r for (r,) in dataset_samples]))
        ).fetchall()
        assert len(result) == 1

        fixed_fields = {
            key: first_dataset_fields[key]
            for key, is_fixed in result[0]._mapping.items()
            if is_fixed
        }
        _LOG.info(
            "product.fixed_metadata_search.done",
            product=product.name,
            sampled_dataset_count=sample_datasets_size,
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
            "product.links.{kind}",
            extra=dict(kind=kind),
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
        product_name: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
    ) -> Optional[TimePeriodOverview]:
        period, start_day = TimePeriodOverview.flat_period_representation(
            year, month, day
        )
        if year and month and day:
            # We don't store days, they're quick.
            return self._summariser.calculate_summary(
                product_name,
                year_month_day=(year, month, day),
                product_refresh_time=datetime.now(),
            )

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

        return _summary_from_row(
            res, product_name=product_name, grouping_timezone=self.grouping_timezone
        )

    def get_all_dataset_counts(
        self,
    ) -> Dict[Tuple[str, int, int], int]:
        """
        Get dataset count for all (product, year, month) combinations.
        """
        res = self._engine.execute(
            select(
                [
                    PRODUCT.c.name,
                    TIME_OVERVIEW.c.start_day,
                    TIME_OVERVIEW.c.period_type,
                    TIME_OVERVIEW.c.dataset_count,
                ]
            )
            .select_from(TIME_OVERVIEW.join(PRODUCT))
            .where(TIME_OVERVIEW.c.product_ref == PRODUCT.c.id)
            .order_by(
                PRODUCT.c.name, TIME_OVERVIEW.c.start_day, TIME_OVERVIEW.c.period_type
            )
        )

        return {
            (
                r.name,
                *TimePeriodOverview.from_flat_period_representation(
                    r.period_type, r.start_day
                )[:2],
            ): r.dataset_count
            for r in res
        }

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
                    PRODUCT.c.last_refresh.label("last_refresh_time"),
                    PRODUCT.c.last_successful_summary.label(
                        "last_successful_summary_time"
                    ),
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
    def products_location_samples_all(
        self, sample_size: int = 50
    ) -> Dict[str, List[ProductLocationSample]]:
        """
        Get sample locations of all products

        This is the same as product_location_samples(), but will be significantly faster
        if you need to fetch all products at once.

        (It's faster because it does only one DB query round-trip instead of N (where N is
         number of products). The latency of repeated round-trips adds up tremendously on
         cloud instances.)
        """
        queries = []
        for dataset_type in self.all_dataset_types():
            subquery = (
                select(
                    [
                        literal(dataset_type.name).label("name"),
                        (
                            ODC_DATASET_LOCATION.c.uri_scheme
                            + ":"
                            + ODC_DATASET_LOCATION.c.uri_body
                        ).label("uri"),
                    ]
                )
                .select_from(ODC_DATASET_LOCATION.join(ODC_DATASET))
                .where(ODC_DATASET.c.dataset_type_ref == dataset_type.id)
                .where(ODC_DATASET.c.archived.is_(None))
                .limit(sample_size)
            )
            queries.append(subquery)

        product_urls = defaultdict(list)
        if queries:  # Don't run invalid SQL on empty database
            for product_name, uri in self._engine.execute(union_all(*queries)):
                product_urls[product_name].append(uri)

        return {
            name: list(_common_paths_for_uris(uris))
            for name, uris in product_urls.items()
        }

    @ttl_cache(ttl=DEFAULT_TTL)
    def product_location_samples(
        self,
        name: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        *,
        sample_size: int = 100,
    ) -> List[ProductLocationSample]:
        """
        Sample some dataset locations for the given product, and return
        the common location.

        Returns one row for each uri scheme found (http, file etc).
        """
        search_args = dict()
        if year or month or day:
            search_args["time"] = _utils.as_time_range(year, month, day)

        # Sample 100 dataset uris
        uri_samples = sorted(
            uri
            for [uri] in self.index.datasets.search_returning(
                ("uri",), product=name, **search_args, limit=sample_size
            )
        )

        return list(_common_paths_for_uris(uri_samples))

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

    def _persist_product_extent(self, product: ProductSummary):
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
            last_refresh=product.last_refresh_time,
        )

        # Dear future reader. This section used to use an 'UPSERT' statement (as in,
        # insert, on_conflict...) and while this works, it triggers the sequence
        # `product_id_seq` to increment as part of the check for insertion. This
        # is bad because there's only 32 k values in the sequence and we have run out
        # a couple of times! So, It appears that this update-else-insert must be done
        # in two transactions...
        row = self._engine.execute(
            select([PRODUCT.c.id, PRODUCT.c.last_refresh]).where(
                PRODUCT.c.name == product.name
            )
        ).fetchone()

        if row:
            # Product already exists, so update it
            row = self._engine.execute(
                PRODUCT.update()
                .returning(PRODUCT.c.id, PRODUCT.c.last_refresh)
                .where(PRODUCT.c.id == row[0])
                .values(fields)
            ).fetchone()
        else:
            # Product doesn't exist, so insert it
            row = self._engine.execute(
                postgres.insert(PRODUCT)
                .returning(PRODUCT.c.id, PRODUCT.c.last_refresh)
                .values(**fields, name=product.name)
            ).fetchone()
        self._product.cache_clear()
        product_id, last_refresh_time = row

        product.id_ = product_id

    def _put(
        self,
        summary: TimePeriodOverview,
    ):
        log = _LOG.bind(
            period=summary.period_tuple,
            summary_count=summary.dataset_count,
        )
        log.info("product.put")
        product = self._product(summary.product_name)
        period, start_day = summary.as_flat_period()

        row = _summary_to_row(summary, grouping_timezone=self.grouping_timezone)
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

    def get_item(
        self, id_: Union[UUID, str], full_dataset: bool = True
    ) -> Optional[DatasetItem]:
        """
        Get a DatasetItem record for the given dataset UUID if it exists.
        """
        items = list(
            self.search_items(
                dataset_ids=[id_], full_dataset=full_dataset, order=ItemSort.UNSORTED
            )
        )
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
        intersects: BaseGeometry = None,
        dataset_ids: Sequence[UUID] = None,
    ) -> Select:
        if dataset_ids is not None:
            query = query.where(DATASET_SPATIAL.c.id.in_(dataset_ids))

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
        if intersects:
            query = query.where(
                func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326).intersects(
                    from_shape(intersects)
                )
            )
        if product_names:
            if len(product_names) == 1:
                query = query.where(
                    DATASET_SPATIAL.c.dataset_type_ref
                    == select([ODC_DATASET_TYPE.c.id])
                    .where(ODC_DATASET_TYPE.c.name == product_names[0])
                    .scalar_subquery()
                )
            else:
                query = query.where(
                    DATASET_SPATIAL.c.dataset_type_ref.in_(
                        select([ODC_DATASET_TYPE.c.id])
                        .where(ODC_DATASET_TYPE.c.name.in_(product_names))
                        .scalar_subquery()
                    )
                )

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
        intersects: BaseGeometry = None,
        limit: int = 500,
        offset: int = 0,
        full_dataset: bool = False,
        dataset_ids: Sequence[UUID] = None,
        order: ItemSort = ItemSort.DEFAULT_SORT,
    ) -> Generator[DatasetItem, None, None]:
        """
        Search datasets using Explorer's spatial table

        Returned as DatasetItem records, with optional embedded full Datasets
        (if full_dataset==True)

        Returned results are always sorted by (center_time, id)
        """
        geom = func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326)

        columns = [
            geom.label("geometry"),
            func.Box2D(geom).cast(String).label("bbox"),
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
            intersects=intersects,
            dataset_ids=dataset_ids,
        )

        # Maybe sort
        if order == ItemSort.DEFAULT_SORT:
            query = query.order_by(DATASET_SPATIAL.c.center_time, DATASET_SPATIAL.c.id)
        elif order == ItemSort.UNSORTED:
            ...  # Nothing! great!
        elif order == ItemSort.RECENTLY_ADDED:
            if not full_dataset:
                raise NotImplementedError(
                    "Only full-dataset searches can be sorted by recently added"
                )
            query = query.order_by(ODC_DATASET.c.added.desc())
        else:
            raise RuntimeError(
                f"Unknown item sort order {order!r} (perhaps this is a bug?)"
            )

        query = query.limit(limit).offset(
            # TODO: Offset/limit isn't particularly efficient for paging...
            offset
        )

        for r in self._engine.execute(query):
            yield DatasetItem(
                dataset_id=r.id,
                bbox=_box2d_to_bbox(r.bbox) if r.bbox else None,
                product_name=self.index.products.get(r.dataset_type_ref).name,
                geometry=(
                    _get_shape(r.geometry, self._get_srid_name(r.geometry.srid))
                    if r.geometry is not None
                    else None
                ),
                region_code=r.region_code,
                creation_time=r.creation_time,
                center_time=r.center_time,
                odc_dataset=(
                    _utils.make_dataset_from_select_fields(self.index, r)
                    if full_dataset
                    else None
                ),
            )

    def _recalculate_period(
        self,
        product: ProductSummary,
        year: Optional[int] = None,
        month: Optional[int] = None,
        product_refresh_time: datetime = None,
    ) -> TimePeriodOverview:
        """Recalculate the given period and store it in the DB"""
        if year and month:
            summary = self._summariser.calculate_summary(
                product.name,
                year_month_day=(year, month, None),
                product_refresh_time=product_refresh_time,
            )
        elif year:
            summary = TimePeriodOverview.add_periods(
                self.get(product.name, year, month_, None) for month_ in range(1, 13)
            )

        # Product. Does it have data?
        elif product.dataset_count > 0:
            summary = TimePeriodOverview.add_periods(
                self.get(product.name, year_, None, None)
                for year_ in range(
                    product.time_earliest.astimezone(self.grouping_timezone).year,
                    product.time_latest.astimezone(self.grouping_timezone).year + 1,
                )
            )
        else:
            summary = TimePeriodOverview.empty(product.name)

        summary.product_refresh_time = product_refresh_time
        summary.period_tuple = (product.name, year, month, None)

        self._put(summary)
        for listener in self._update_listeners:
            listener(
                product_name=product.name,
                year=year,
                month=month,
                day=None,
                summary=summary,
            )
        return summary

    def refresh(
        self,
        product_name: str,
        force: bool = False,
        recreate_dataset_extents: bool = False,
        reset_incremental_position: bool = False,
        minimum_change_scan_window: timedelta = None,
    ) -> Tuple[GenerateResult, TimePeriodOverview]:
        """
        Update Explorer's information and summaries for a product.

        This will scan for any changes since the last run, update
        the spatial extents and any outdated time summaries.

        :param minimum_change_scan_window: Always rescan this window of time for dataset changes,
                    even if the refresh tool has run more recently.

                    This is useful if you have something that doesn't make rows visible immediately,
                    such as a sync service from another location.
        :param product_name: ODC Product name
        :param force: Recreate everything, even if it doesn't appear to have changed.
        :param recreate_dataset_extents: Force-recreate just the spatial/extent table (including
                       removing deleted datasets). This is significantly faster than "force", but
                       doesn't update time summaries.
        :param reset_incremental_position: Ignore the current incremental-update marker position,
                       and run with a more conservative position: when the last dataset was
                       added to Explorer's tables, rather than when the last refresh was successful.

                       This is primarily useful for developers who restore from backups, whose Explorer
                       tables will be out of sync with a restored, newer ODC database.
        """
        log = _LOG.bind(product_name=product_name)

        old_product: ProductSummary = self.get_product_summary(product_name)

        # Which datasets to scan for updates?
        if (
            # If they've never summarised this product before
            (old_product is None)
            # ... Or it's an old Explorer from before incremental-updates were added.
            or (old_product.last_successful_summary_time is None)
            # Or we're using brute force
            or force
        ):
            # "No limit". Recompute all.
            only_datasets_newer_than = None

        # Otherwise, do they want to reset the incremental position?
        # -> Find the most recently indexed dataset that has touched our own spatial table,
        #    and only scan changes from that time onward.
        #    (this will be more expensive than normal incremental [below], as it may scan a
        #     lot more datasets, not just the ones from the last generate run.)
        elif reset_incremental_position:
            only_datasets_newer_than = self._newest_known_dataset_addition_time(
                product_name
            )
        else:
            # Otherwise only refresh datasets newer than the last successful run.
            only_datasets_newer_than = (
                old_product.last_successful_summary_time
                - self.dataset_overlap_carefulness
            )

        # If there's a minimum window to scan, make sure we fill it.
        if minimum_change_scan_window and only_datasets_newer_than:
            only_datasets_newer_than = min(
                only_datasets_newer_than,
                self._database_time_now() - minimum_change_scan_window,
            )

        extent_changes, new_product = self.refresh_product_extent(
            product_name,
            scan_for_deleted=recreate_dataset_extents or force,
            only_those_newer_than=(
                None if recreate_dataset_extents else only_datasets_newer_than
            ),
        )
        log.info("extent.refresh.done", changed=extent_changes)

        refresh_timestamp = new_product.last_refresh_time
        assert refresh_timestamp is not None

        # What month summaries do we need to generate?

        # If we're scanning all of them...
        if only_datasets_newer_than is None:
            # Then choose the whole time range of the product to generate.
            log.info("product.generate_whole_range")
            if force:
                log.warning("forcing_refresh")

            # Regenerate the old months too, in case any have been deleted.
            old_months = self._already_summarised_months(product_name)

            months_to_update = sorted(
                (month, "all")
                for month in old_months.union(
                    new_product.iter_months(self.grouping_timezone)
                )
            )
            refresh_type = GenerateResult.CREATED

        # Otherwise, only regenerate the ones that changed.
        else:
            log.info("product.incremental_update")
            months_to_update = self.find_months_needing_update(
                product_name, only_datasets_newer_than
            )
            refresh_type = GenerateResult.UPDATED

        # Months
        for change_month, new_count in months_to_update:
            log.debug(
                "product.month_refresh",
                product=product_name,
                month=change_month,
                change_count=new_count,
            )
            self._recalculate_period(
                new_product,
                change_month.year,
                change_month.month,
                product_refresh_time=refresh_timestamp,
            )

        # Find year records who are older than their month records
        #   (This will find any months calculated above, as well
        #    as from previous interrupted runs.)
        years_to_update = self.find_years_needing_update(product_name)
        for year in years_to_update:
            self._recalculate_period(
                new_product,
                year,
                product_refresh_time=refresh_timestamp,
            )

        # Now update the whole-product record
        updated_summary = self._recalculate_period(
            new_product,
            product_refresh_time=refresh_timestamp,
        )
        _LOG.info(
            "product.complete!",
            product_name=new_product.name,
            previous_refresh_time=new_product.last_successful_summary_time,
            new_refresh_time=refresh_timestamp,
        )
        self._mark_product_refresh_completed(new_product, refresh_timestamp)

        # If nothing changed?
        if (
            (not extent_changes)
            and (not months_to_update)
            and (not years_to_update)
            # ... and it already existed:
            and old_product
        ):
            refresh_type = GenerateResult.NO_CHANGES

        return refresh_type, updated_summary

    def _already_summarised_months(self, product_name: str) -> Set[date]:
        """Get all months that have a recorded summary already for this product"""

        existing_product = self.get_product_summary(product_name)
        if not existing_product:
            return set()

        return {
            r.start_day
            for r in self._engine.execute(
                select([TIME_OVERVIEW.c.start_day]).where(
                    TIME_OVERVIEW.c.product_ref == existing_product.id_
                )
            )
        }

    def _database_time_now(self) -> datetime:
        """
        What's the current time according to the database?

        Any change timestamps stored in the database are using database-local
        time, which could be different to the time on this current machine!
        """
        return self._engine.execute(select([func.now()])).scalar()

    def _newest_known_dataset_addition_time(self, product_name) -> datetime:
        """
        Of all the datasets that are present in Explorer's own tables, when
        was the most recent one indexed to ODC?
        """
        return self._engine.execute(
            select([func.max(ODC_DATASET.c.added)])
            .select_from(
                DATASET_SPATIAL.join(
                    ODC_DATASET, onclause=DATASET_SPATIAL.c.id == ODC_DATASET.c.id
                )
            )
            .where(
                DATASET_SPATIAL.c.dataset_type_ref
                == self.get_dataset_type(product_name).id
            )
        ).scalar()

    def _mark_product_refresh_completed(
        self, product: ProductSummary, refresh_timestamp: datetime
    ):
        """
        Mark the product as successfully refreshed at the given product-table timestamp

        (so future runs will be incremental from this point onwards)
        """
        assert product.id_ is not None
        self._engine.execute(
            PRODUCT.update()
            .where(PRODUCT.c.id == product.id_)
            .where(
                or_(
                    PRODUCT.c.last_successful_summary.is_(None),
                    PRODUCT.c.last_successful_summary < refresh_timestamp.isoformat(),
                )
            )
            .values(last_successful_summary=refresh_timestamp)
        )
        self._product.cache_clear()

    @lru_cache()
    def _get_srid_name(self, srid: int):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        return get_srid_name(self._engine, srid)

    def list_complete_products(self) -> List[str]:
        """
        List all names of products that have summaries available.
        """
        return sorted(
            product.name
            for product in self.all_dataset_types()
            if self.has(product.name, None, None, None)
        )

    def find_datasets_for_region(
        self,
        product_name: str,
        region_code: str,
        year: int,
        month: int,
        day: int,
        limit: int,
        offset: int = 0,
    ) -> Iterable[Dataset]:
        time_range = _utils.as_time_range(
            year, month, day, tzinfo=self.grouping_timezone
        )
        return _extents.datasets_by_region(
            self._engine,
            self.index,
            product_name,
            region_code,
            time_range,
            limit,
            offset=offset,
        )

    def find_products_for_region(
        self,
        region_code: str,
        year: int,
        month: int,
        day: int,
        limit: int,
        offset: int = 0,
    ) -> Iterable[DatasetType]:
        time_range = _utils.as_time_range(
            year, month, day, tzinfo=self.grouping_timezone
        )
        return _extents.products_by_region(
            self._engine,
            self.index,
            region_code,
            time_range,
            limit,
            offset=offset,
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
    refresh_products = recompute_dataset_extents or (
        PleaseRefresh.PRODUCTS in please_refresh
    )

    if refresh_products:
        for dt in store.all_dataset_types():
            name = dt.name
            # Skip product if it's never been summarised at all.
            if store.get_product_summary(name) is None:
                continue

            if recompute_dataset_extents:
                _LOG.info("data.refreshing_extents", product=name)
            store.refresh_product_extent(
                name,
                scan_for_deleted=recompute_dataset_extents,
            )
    _LOG.info("data.refreshing_extents.complete")


def _safe_read_date(d):
    if d:
        return _utils.default_utc(dateutil.parser.parse(d))

    return None


def _summary_from_row(res, product_name, grouping_timezone=default_timezone):
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
    period_type = res["period_type"]
    year, month, day = TimePeriodOverview.from_flat_period_representation(
        period_type, res["start_day"]
    )

    return TimePeriodOverview(
        product_name=product_name,
        year=year,
        month=month,
        day=day,
        dataset_count=res["dataset_count"],
        # : Counter
        timeline_dataset_counts=timeline_dataset_counts,
        region_dataset_counts=region_dataset_counts,
        timeline_period=res["timeline_period"],
        # : Range
        time_range=(
            Range(
                (
                    res["time_earliest"].astimezone(grouping_timezone)
                    if res["time_earliest"]
                    else res["time_earliest"]
                ),
                (
                    res["time_latest"].astimezone(grouping_timezone)
                    if res["time_latest"]
                    else res["time_latest"]
                ),
            )
            if res["time_earliest"]
            else None
        ),
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
        product_refresh_time=res["product_refresh_time"],
        # When this summary was last generated
        summary_gen_time=res["generation_time"],
        crses=set(res["crses"]) if res["crses"] is not None else None,
    )


def _summary_to_row(
    summary: TimePeriodOverview, grouping_timezone=default_timezone
) -> dict:
    day_values, day_counts = _counter_key_vals(summary.timeline_dataset_counts)
    region_values, region_counts = _counter_key_vals(summary.region_dataset_counts)

    begin, end = summary.time_range if summary.time_range else (None, None)

    if summary.footprint_geometry and summary.footprint_srid is None:
        raise ValueError("Geometry without srid", summary)
    if summary.product_refresh_time is None:
        raise ValueError("Product has no refresh time??", summary)
    return dict(
        dataset_count=summary.dataset_count,
        timeline_dataset_start_days=day_values,
        timeline_dataset_counts=day_counts,
        # TODO: SQLAlchemy needs a bit of type help for some reason. Possible PgGridCell bug?
        regions=func.cast(region_values, type_=postgres.ARRAY(String)),
        region_dataset_counts=region_counts,
        timeline_period=summary.timeline_period,
        time_earliest=begin.astimezone(grouping_timezone) if begin else begin,
        time_latest=end.astimezone(grouping_timezone) if end else end,
        size_bytes=summary.size_bytes,
        product_refresh_time=summary.product_refresh_time,
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


def _common_paths_for_uris(
    uri_samples: Iterator[str],
) -> Generator[ProductLocationSample, None, None]:
    """
    >>> list(_common_paths_for_uris(['file:///a/thing-1.txt', 'file:///a/thing-2.txt', 'file:///a/thing-3.txt']))
    [ProductLocationSample(uri_scheme='file', common_prefix='file:///a/', example_uris=['file:///a/thing-1.txt', \
'file:///a/thing-2.txt', 'file:///a/thing-3.txt'])]
    """

    def uri_scheme(uri: str):
        return uri.split(":", 1)[0]

    for scheme, uri_grouper in groupby(sorted(uri_samples), uri_scheme):
        uris = list(uri_grouper)

        # Use the first, last and middle as examples
        # (they're sorted, so this shows diversity)
        example_uris = {uris[0], uris[-1], uris[int(len(uris) / 2)]}
        #              ⮤ we use a set for when len < 3

        yield ProductLocationSample(
            scheme, _utils.common_uri_prefix(uris), sorted(example_uris)
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
