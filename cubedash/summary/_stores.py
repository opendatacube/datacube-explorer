import math
import re
from collections import Counter
from collections.abc import Generator, Iterable, Sequence
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from enum import Enum, auto
from itertools import groupby
from typing import Any, Literal, Protocol
from uuid import UUID
from zoneinfo import ZoneInfo

import structlog
from cachetools.func import ttl_cache
from eodatasets3.stac import MAPPING_EO3_TO_STAC
from geoalchemy2 import WKBElement
from geoalchemy2 import shape as geo_shape
from geoalchemy2.shape import from_shape, to_shape
from odc.geo import BoundingBox, MaybeCRS
from pygeofilter.backends.sqlalchemy.evaluate import (
    SQLAlchemyFilterEvaluator as FilterEvaluator,
)
from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from shapely.geometry.base import BaseGeometry
from sqlalchemy import RowMapping, func, select
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import Select
from sqlalchemy.sql.ddl import CreateSchema, DropSchema

try:
    from cubedash._version import version as explorer_version
except ModuleNotFoundError:
    explorer_version = "ci-test-pipeline"
from datacube.index import Index
from datacube.model import Dataset, Field, MetadataType, Product, Range
from odc.geo.geom import Geometry

from cubedash import _utils
from cubedash.index import EmptyDbError, ExplorerIndex
from cubedash.index.postgis import ExplorerPgisIndex
from cubedash.index.postgres import ExplorerPgIndex
from cubedash.summary import RegionInfo, TimePeriodOverview, _extents, _schema
from cubedash.summary._extents import ProductArrival, RegionSummary
from cubedash.summary._schema import PleaseRefresh
from cubedash.summary._summarise import DEFAULT_TIMEZONE, Summariser

DEFAULT_TTL = 90

_DEFAULT_REFRESH_OLDER_THAN = timedelta(hours=23)

_LOG = structlog.stdlib.get_logger()

# The default grouping epsg code to use on init of a new Explorer schema.
#
# We'll use a global equal area.
DEFAULT_EPSG = 6933

default_timezone = ZoneInfo(DEFAULT_TIMEZONE)


def explorer_index(index: Index) -> ExplorerIndex:
    if index.name == "pg_index":
        return ExplorerPgIndex(index)
    if index.name == "pgis_index":
        return ExplorerPgisIndex(index)
    # should we permit memory? default to postgres? other handling?
    raise ValueError(f"Cannot run explorer with index {index.name}")


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
    # Tuple of (time_earliest, time_latest), None when dataset_count == 0.
    duration: tuple[datetime, datetime] | None

    source_products: list[str]
    derived_products: list[str]

    # Metadata values that are the same on every dataset.
    # (on large products this is judged via sampling, so may not be 100%)
    fixed_metadata: dict[str, str | float | int | datetime]

    # The db-server-local time when this product record+extent was refreshed.
    last_refresh_time: datetime

    # The `last_refresh_time` last time when summary generation was last fully completed.
    # (To find changes, we'll scan any datasets newer than this date)
    last_successful_summary_time: datetime | None = None

    # Not recommended for use by users, as ids are local and internal.
    # The 'name' is typically used as an identifier, and with ODC itself.
    id_: int | None = None

    def iter_months(self, grouping_timezone: tzinfo) -> Generator[date]:
        """
        Iterate through all months in its time range.
        """
        if self.dataset_count == 0 or self.duration is None:
            return

        time_earliest, time_latest = self.duration
        start = time_earliest.astimezone(grouping_timezone)
        end = time_latest.astimezone(grouping_timezone)
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
    geometry: Geometry | None
    region_code: str
    creation_time: datetime
    center_time: datetime
    odc_dataset: Dataset | None = None

    @property
    def geom_geojson(self) -> dict | None:
        if self.geometry is None:
            return None
        return self.geometry.__geo_interface__

    def as_geojson(self) -> dict:
        return {
            "id": self.dataset_id,
            "type": "Feature",
            "bbox": self.bbox,
            "geometry": self.geom_geojson,
            "properties": {
                "datetime": self.center_time,
                "odc:product": self.product_name,
                "odc:processing_datetime": self.creation_time,
                "cubedash:region_code": self.region_code,
            },
        }


@dataclass
class CollectionItem:
    name: str
    definition: dict[str, Any]
    time_earliest: datetime | None
    time_latest: datetime | None
    bbox: BoundingBox | None

    @property
    def title(self) -> str:
        metadata = self.definition.get("metadata")
        if metadata is not None and "title" in metadata:
            return metadata["title"]
        return self.name

    @property
    def description(self) -> str | None:
        return self.definition.get("description")


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
    example_uris: list[str]


class ChangeListener(Protocol):
    def __call__(
        self,
        product_name: str,
        year: int | None,
        month: int | None,
        day: int | None,
        summary: TimePeriodOverview | None,
    ) -> None: ...


class SummaryStore:
    def __init__(
        self, e_index: ExplorerIndex, summariser: Summariser, log=_LOG
    ) -> None:
        self.e_index = e_index
        self.index = e_index.index
        self.log = log
        self._update_listeners: list[ChangeListener] = []

        self._summariser = summariser

        # How much extra time to include in incremental update scans?
        #    The incremental-updater searches for any datasets with a newer change-timestamp than
        #    its last successful run. But some earlier-timestamped datasets may not have been
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

    def add_change_listener(self, listener: ChangeListener) -> None:
        self._update_listeners.append(listener)

    def is_initialised(self) -> bool:
        """
        Do our DB schemas exist?
        """
        return self.e_index.schema_initialised()

    def is_schema_compatible(self, for_writing_operations_too: bool) -> bool:
        """
        Have all schema updates been applied?
        """
        postgis_ver, is_compatible = self.e_index.schema_compatible_info(
            for_writing_operations_too
        )
        _LOG.debug("software.version", postgis=postgis_ver, explorer=explorer_version)
        return is_compatible

    def init(self, grouping_epsg_code: int | None) -> bool:
        """
        Initialise any schema elements that don't exist.

        Takes an epsg_code, of the CRS used internally for summaries.

        (Requires `create` permissions in the db)
        """
        try:
            self.e_index.execute_ddl(
                CreateSchema(_schema.CUBEDASH_SCHEMA, if_not_exists=True)
            )
            refresh_also = self.e_index.init_schema(grouping_epsg_code or DEFAULT_EPSG)
        except ProgrammingError as e:
            _LOG.error(str(e))
            return False
        if refresh_also:
            # Refresh product information after a schema update, plus the given kind of data.
            for product in self.all_products():
                name = product.name
                # Skip product if it's never been summarised at all.
                if self.get_product_summary(name) is None:
                    continue
                _LOG.info("data.refreshing_extents", product=name)
                self.refresh_product_extent(
                    name,
                    scan_for_deleted=PleaseRefresh.DATASET_EXTENTS
                    in refresh_also,  # I believe this is always True
                )
            _LOG.info("data.refreshing_extents.complete")
        return True

    @classmethod
    def create(
        cls, index: Index, log=_LOG, grouping_time_zone: str = DEFAULT_TIMEZONE
    ) -> "SummaryStore":
        e_index = explorer_index(index)
        return cls(
            e_index, Summariser(e_index, grouping_time_zone=grouping_time_zone), log=log
        )

    def close(self) -> None:  # do we still need this?
        """Close any pooled/open connections. Necessary before forking.

        Also useful during testing.
        """
        # This is going to do the same .dispose() twice, but, it's a noop the second time
        # and will be safer until we can tidy up handling of the SQLAlchemy connections
        self.index.close()
        self.e_index.engine.dispose()

    def refresh_all_product_extents(self) -> None:
        for product in self.all_products():
            self.refresh_product_extent(product.name)
        self.refresh_stats()

    def find_months_needing_update(
        self, product_name: str, only_those_newer_than: datetime
    ) -> list[tuple[date, int]]:
        """
        What months have had dataset changes since they were last generated?
        """
        product = self.get_product(product_name)

        # Find the most-recently updated datasets and group them by month.
        return sorted(
            (month.date(), count)
            for month, count in self.e_index.outdated_months(
                product, only_those_newer_than
            )
        )

    def find_years_needing_update(self, product_name: str) -> list[int]:
        """
        Find any years that need to be generated.

        Either:
           1) They don't exist yet, or
           2) They existed before and has been deleted or archived, or
           3) They have month-records that are newer than our year-record.
        """
        product = self.get_product_summary(product_name)
        if product is None or product.id_ is None:
            return []

        # Years that have already been summarised
        summarised_years = {
            r[0].year
            for r in self.e_index.already_summarised_period("year", product.id_)
        }

        # Empty product? No years
        if product.dataset_count == 0 or product.duration is None:
            # check if the timeoverview needs cleanse
            return list(summarised_years)

        time_earliest, time_latest = product.duration
        # All years we are expected to have
        expected_years = set(
            range(
                time_earliest.astimezone(self.grouping_timezone).year,
                time_latest.astimezone(self.grouping_timezone).year + 1,
            )
        )

        missing_years = expected_years.difference(summarised_years)

        # Years who have month-records updated more recently than their own record.
        outdated_years = {
            start_day.year for [start_day] in self.e_index.outdated_years(product.id_)
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

        most_recent_change = self.index.products.most_recent_change(product_name)
        has_new_changes = most_recent_change is not None and (
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
        only_those_newer_than: datetime | None = None,
        force: bool = False,
    ) -> tuple[int, ProductSummary] | None:
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

        product = self.get_product(product_name)

        _LOG.info("init.product", product_name=product.name)
        change_count = _extents.refresh_spatial_extents(
            self.e_index,
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

        if product.id is None:
            return None
        # if change_count or force_dataset_extent_recompute:
        earliest, latest, total_count = self.e_index.product_time_overview(product.id)

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
            (earliest, latest),
            source_products=source_products,
            derived_products=derived_products,
            fixed_metadata=fixed_metadata,
            last_refresh_time=covers_up_to,
        )

        # TODO: This is an expensive operation. We regenerate them all every time there are changes.
        log = _LOG.bind(product_name=product.name)  # , engine=str(self._engine))
        log.info("refresh.regions.start")
        log.info("refresh.regions.update.count.and.insert.new")
        result = self.e_index.upsert_product_regions(product.id)
        log.info("refresh.regions.inserted", result=list(result))
        log.info(
            "refresh.regions.update.count.and.insert.new.end",
            changed_rows=result.rowcount,
        )
        # delete region rows with no related datasets in dataset_spatial table
        log.info("refresh.regions.delete.empty.regions")
        result = self.e_index.delete_product_empty_regions(product.id)
        log.info("refresh.regions.delete.empty.regions.end")
        log.info("refresh.regions.end", changed_regions=result.rowcount)

        self._persist_product_extent(new_summary)
        return change_count, new_summary

    def refresh_stats(self, concurrently: bool = False) -> None:
        """
        Refresh general statistics tables that cover all products.

        This is ideally done once after all needed products have been refreshed.
        """
        self.e_index.refresh_stats(concurrently)

    def _find_product_fixed_metadata(
        self, product: Product, sample_datasets_size: int
    ) -> dict[str, Any]:
        """
        Find metadata fields that have an identical value in every dataset of the product.

        This is expensive, so only the given percentage of datasets will be sampled (but
        feel free to sample 100%!)

        """
        # Get a single dataset, then we'll compare the rest against its values.
        first_dataset_fields = next(
            iter(self.index.datasets.search(product=product.name, limit=1))
        ).metadata.fields

        simple_field_types = {"string", "numeric", "double", "integer", "datetime"}

        candidate_fields: list[tuple[str, Field]] = [
            (name, field)
            for name, field in self.e_index.get_mutable_dataset_search_fields(
                product.metadata_type
            ).items()
            if field.type_name in simple_field_types and name in first_dataset_fields
        ]

        dataset_samples = self.e_index.ds_search_returning(
            ["id"],
            limit=sample_datasets_size,
            order_by=[func.random()],
            args={"product": product.name},
        )

        _LOG.info(
            "product.fixed_metadata_search",
            product=product.name,
            sampled_dataset_count=sample_datasets_size,
        )
        result = self.e_index.find_fixed_columns(
            first_dataset_fields, candidate_fields, dataset_samples
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
        self,
        product: Product,
        kind: Literal["source", "derived"],
        sample_percentage: float,
    ) -> list[str]:
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
        if product.id is None:
            return []
        # Avoid tablesample (full table scan) when we're getting all of the product anyway.
        sample_sql = ""
        if sample_percentage < 100:
            sample_sql = f"tablesample system ({sample_percentage})"

        rv = self.e_index.linked_products_search(
            product.id, sample_sql, kind
        ).fetchone()
        linked_product_names = [] if rv is None else rv[0]
        _LOG.info(
            "product.links.{kind}",
            extra={"kind": kind},
            product=product.name,
            linked=linked_product_names,
            sample_percentage=round(sample_percentage, 2),
        )
        return list(linked_product_names or [])

    def drop_all(self) -> None:
        """
        Drop all cubedash-specific tables/schema.
        """
        self.e_index.execute_ddl(
            DropSchema(_schema.CUBEDASH_SCHEMA, cascade=True, if_exists=True)
        )

    def get(
        self,
        product_name: str,
        year: int | None = None,
        month: int | None = None,
        day: int | None = None,
    ) -> TimePeriodOverview | None:
        period, start_day = TimePeriodOverview.flat_period_representation(
            year, month, day
        )
        if year and month and day:
            # We don't store days, they're quick.
            return self._summariser.calculate_summary(
                product_name, year, month, day, datetime.now()
            )

        product = self.get_product_summary(product_name)
        if product is None or product.id_ is None:
            return None

        res = self.e_index.product_time_summary(
            product.id_, start_day, period
        ).fetchone()

        if not res:
            return None
        return _summary_from_row(
            res._mapping,
            product_name=product_name,
            grouping_timezone=self.grouping_timezone,
        )

    def get_all_dataset_counts(self) -> dict[tuple[str, int | None, int | None], int]:
        """
        Get dataset count for all (product, year, month) combinations.
        """
        res = self.e_index.product_ds_count_per_period()

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
    def all_products(self) -> Sequence[Product]:
        return tuple(self.index.products.get_all())

    @ttl_cache(ttl=DEFAULT_TTL)
    def all_metadata_types(self) -> Sequence[MetadataType]:
        return tuple(self.index.metadata_types.get_all())

    @ttl_cache(ttl=DEFAULT_TTL)
    def get_product(self, name: str) -> Product:
        for d in self.all_products():
            if d.name == name:
                return d
        raise KeyError(f"Unknown product {name!r}")

    @ttl_cache(ttl=DEFAULT_TTL)
    def get_metadata_type(self, name: str) -> MetadataType:
        for d in self.all_metadata_types():
            if d.name == name:
                return d
        raise KeyError(f"Unknown metadata type {name!r}")

    @ttl_cache(ttl=DEFAULT_TTL)
    def _product_by_id(self, id_: int) -> Product:
        for d in self.all_products():
            if d.id == id_:
                return d
        raise KeyError(f"Unknown product id {id_!r}")

    @ttl_cache(ttl=DEFAULT_TTL)
    def _product(self, name: str) -> ProductSummary:
        row = self.e_index.product_summary_cols(name)
        if not row:
            raise ValueError(f"Unknown product {name!r} (initialised?)")
        row = dict(row._mapping)
        source_products = [
            self._product_by_id(id_).name for id_ in row.pop("source_product_refs")
        ]
        derived_products = [
            self._product_by_id(id_).name for id_ in row.pop("derived_product_refs")
        ]
        time_earliest: datetime | None = row.pop("time_earliest")
        time_latest: datetime | None = row.pop("time_latest")

        return ProductSummary(
            name=name,
            duration=None
            if time_earliest is None or time_latest is None
            else (time_earliest, time_latest),
            source_products=source_products,
            derived_products=derived_products,
            **row,
        )

    @ttl_cache(ttl=DEFAULT_TTL)
    def products_location_samples_all(
        self, sample_size: int = 50
    ) -> dict[str, list[ProductLocationSample]]:
        """
        Get sample locations of all products

        This is the same as product_location_samples(), but will be significantly faster
        if you need to fetch all products at once.

        (It's faster because it does only one DB query round-trip instead of N (where N is
         number of products). The latency of repeated round-trips adds up tremendously on
         cloud instances.)
        """
        product_urls = {}
        try:
            for product_name, uris in self.e_index.all_products_location_samples(
                self.all_products(), sample_size
            ):
                if uris is not None:
                    product_urls[product_name] = list(_common_paths_for_uris(uris))
            return product_urls
        except EmptyDbError:
            return {}

    @ttl_cache(ttl=DEFAULT_TTL)
    def product_location_samples(
        self,
        name: str,
        year: int | None = None,
        month: int | None = None,
        day: int | None = None,
        *,
        sample_size: int = 100,
    ) -> list[ProductLocationSample]:
        """
        Sample some dataset locations for the given product, and return
        the common location.

        Returns one row for each uri scheme found (http, file etc.).
        """
        search_args: dict[str, str | Range] = {"product": name}
        if year or month or day:
            time = _utils.as_time_range(year, month, day)
            assert time is not None
            search_args["time"] = time
        # Sample 100 dataset uris
        uri_samples = sorted(
            uri
            for [uri] in self.e_index.ds_search_returning(
                fields=("uri",), limit=sample_size, args=search_args
            )
            if uri is not None
        )

        return list(_common_paths_for_uris(uri_samples))

    def get_quality_stats(self) -> Generator[dict]:
        stats = self.e_index.select_spatial_stats()
        for s in stats:
            row = s._mapping
            d = dict(row)
            d["product"] = self._product_by_id(row["product_ref"])
            d["avg_footprint_bytes"] = (
                row["footprint_size"] / row["count"] if row["footprint_size"] else 0
            )
            yield d

    def get_product_summary(self, name: str) -> ProductSummary | None:
        try:
            return self._product(name)
        except ValueError:
            return None

    @property
    def grouping_timezone(self) -> tzinfo:
        """Timezone used for day/month/year grouping."""
        return ZoneInfo(self._summariser.grouping_time_zone)

    def _persist_product_extent(self, product: ProductSummary) -> None:
        source_product_ids = [
            self.get_product(name).id for name in product.source_products
        ]
        derived_product_ids = [
            self.get_product(name).id for name in product.derived_products
        ]
        time_earliest = None if product.duration is None else product.duration[0]
        time_latest = None if product.duration is None else product.duration[1]
        fields = {
            "dataset_count": product.dataset_count,
            "time_earliest": time_earliest,
            "time_latest": time_latest,
            "source_product_refs": source_product_ids,
            "derived_product_refs": derived_product_ids,
            "fixed_metadata": product.fixed_metadata,
            "last_refresh": product.last_refresh_time,
        }

        row = self.e_index.upsert_product_record(product.name, fields)
        self._product.cache_clear()
        product_id, _ = row

        product.id_ = product_id

    def _put(self, summary: TimePeriodOverview) -> None:
        if summary.footprint_geometry and summary.footprint_srid is None:
            raise ValueError("Geometry without srid", summary)
        if summary.product_refresh_time is None:
            raise ValueError("Product has no refresh time??", summary)
        log = _LOG.bind(
            period=summary.period_tuple, summary_count=summary.dataset_count
        )
        log.info("product.put")
        product_id = self._product(summary.product_name).id_
        if product_id is None:
            return
        period, start_day = summary.as_flat_period()

        day_values, day_counts = _counter_key_vals(summary.timeline_dataset_counts)
        region_values, region_counts = _counter_key_vals(summary.region_dataset_counts)
        begin, end = summary.time_range if summary.time_range else (None, None)
        ret = self.e_index.put_summary(
            product_id,
            start_day,
            period,
            {
                "dataset_count": summary.dataset_count,
                "timeline_dataset_start_days": day_values,
                "timeline_dataset_counts": day_counts,
                "regions": region_values,
                "region_dataset_counts": region_counts,
                "timeline_period": summary.timeline_period,
                "time_earliest": begin.astimezone(self.grouping_timezone)
                if begin
                else None,
                "time_latest": end.astimezone(self.grouping_timezone) if end else None,
                "size_bytes": summary.size_bytes,
                "product_refresh_time": summary.product_refresh_time,
                "footprint_geometry": (
                    None
                    if summary.footprint_geometry is None
                    or summary.footprint_srid is None
                    else geo_shape.from_shape(
                        summary.footprint_geometry, summary.footprint_srid
                    )
                ),
                "footprint_count": summary.footprint_count,
                "generation_time": func.now(),
                "newest_dataset_creation_time": summary.newest_dataset_creation_time,
                "crses": summary.crses,
            },
        ).fetchone()
        if ret is not None:
            summary.summary_gen_time = ret[0]

    def has(
        self, product_name: str, year: int | None, month: int | None, day: int | None
    ) -> bool:
        return self.get(product_name, year, month, day) is not None

    def get_item(self, id_: UUID, full_dataset: bool = True) -> DatasetItem | None:
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

    def get_collection(self, name: str) -> CollectionItem | None:
        collections = list(self.search_collections(name=name))
        if not collections:
            return None
        # collection name should function as a unique identifier
        if len(collections) > 1:
            raise RuntimeError(
                "Something is wrong: Multiple collection results for the same name"
            )
        [collection] = collections
        return collection

    def _add_fields_to_query(
        self,
        query: Select,
        field_exprs,
        product_names: list[str] | None,
        time: tuple[datetime, datetime] | None,
        bbox: tuple[float, float, float, float] | None,
        intersects: BaseGeometry | None,
        dataset_ids: Sequence[UUID] | None,
    ) -> Select:
        if dataset_ids is not None:
            query = query.where(field_exprs["id"].in_(dataset_ids))

        if time:
            query = query.where(
                func.tstzrange(
                    _utils.default_utc(time[0]),
                    _utils.default_utc(time[1]),
                    "[]",
                    type_=TSTZRANGE,
                ).contains(field_exprs["datetime"])
            )

        if bbox:
            query = query.where(
                field_exprs["geometry"].intersects(func.ST_MakeEnvelope(*bbox))
            )
        if intersects:
            query = query.where(
                field_exprs["geometry"].intersects(from_shape(intersects))
            )
        if product_names:
            query = query.where(field_exprs["collection"].in_(product_names))

        return query

    def _get_field_exprs(self, product_names: list[str] | None) -> dict[str, Any]:
        """
        Map properties to their sqlalchemy expressions.
        Allow for properties to be provided as their STAC property name (ex: created),
        their eo3 property name (ex: odc:processing_datetime),
        or their searchable field name as defined by the metadata type (ex: creation_time).
        """
        products = (
            {self.get_product(name) for name in product_names}
            if product_names
            else set(self.all_products())
        )
        field_exprs = {}
        for product in products:
            # aren't these tied to the ODC_DATASET schema?
            # could we handle 'added' in here?
            for value in self.e_index.get_mutable_dataset_search_fields(
                product.metadata_type
            ).values():
                expr = value.alchemy_expression  # type: ignore[attr-defined]
                if hasattr(value, "offset"):
                    field_exprs[value.offset[-1]] = expr
                field_exprs[value.name] = expr

        # add stac property names as well
        for k, v in MAPPING_EO3_TO_STAC.items():
            try:
                # map to same alchemy expression as the eo3 counterparts
                field_exprs[v] = field_exprs[k]
            except KeyError:
                continue
        # manually add fields that aren't included in the metadata search fields
        field_exprs.update(self.e_index.dataset_spatial_field_exprs())
        return field_exprs

    def _add_filter_to_query(
        self,
        query: Select,
        field_exprs: dict[str, Any],
        filter_lang: str | None,
        filter_cql: str | dict | None,
    ) -> Select:
        # use pygeofilter's SQLAlchemy integration to construct the filter query
        filter_cql = (
            parse_cql2_text(filter_cql)
            if filter_lang == "cql2-text"
            else parse_cql2_json(filter_cql)
        )
        return query.filter(FilterEvaluator(field_exprs, True).evaluate(filter_cql))

    def _add_order_to_query(
        self, query: Select, field_exprs: dict[str, Any], sortby: list[dict[str, str]]
    ) -> Select:
        order_clauses = []
        for s in sortby:
            f = s.get("field")
            if f is None:
                continue
            field = field_exprs.get(f)
            # is there any way to check if sortable?
            if field is not None:
                asc = s.get("direction") == "asc"
                if asc:
                    order_clauses.append(field.asc())
                else:
                    order_clauses.append(field.desc())
            # there is no field by that name, ignore
            # the spec does not specify a handling directive for unspecified fields,
            # so we've chosen to ignore them to be in line with the other extensions
        return query.order_by(*order_clauses)

    @ttl_cache(ttl=DEFAULT_TTL)
    def get_arrivals(
        self, period_length: timedelta
    ) -> list[tuple[date, list[ProductArrival]]]:
        """
        Get a list of products with newly added datasets for the last few days.
        """
        current_day = None
        products: list[ProductArrival] = []
        out_groups = []
        for day, product_name, count, dataset_ids in self.e_index.latest_arrivals(
            period_length
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
        product_names: list[str] | None,
        time: tuple[datetime, datetime] | None,
        bbox: tuple[float, float, float, float] | None,
        intersects: BaseGeometry | None,
        dataset_ids: Sequence[UUID] | None,
        filter_lang: str | None,
        filter_cql: str | dict | None,
    ) -> int:
        """
        Do the base select query to get the count of matching datasets.
        """
        # to account the possibility of 'collection' in the filter
        query = self.e_index.spatial_select_query([func.count()], full=bool(filter_cql))

        field_exprs = self._get_field_exprs(product_names)
        query = self._add_fields_to_query(
            query,
            field_exprs,
            product_names=product_names,
            time=time,
            bbox=bbox,
            intersects=intersects,
            dataset_ids=dataset_ids,
        )

        if filter_cql:
            query = self._add_filter_to_query(
                query, field_exprs, filter_lang, filter_cql
            )
        result = self.e_index.execute_query(query)

        if len(result) != 0:
            return result[0][0]
        return 0

    def search_items(
        self,
        *,
        product_names: list[str] | None = None,
        time: tuple[datetime, datetime] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        intersects: BaseGeometry | None = None,
        limit: int = 500,
        offset: int = 0,
        full_dataset: bool = False,
        dataset_ids: Sequence[UUID] | None = None,
        filter_lang: str | None = None,
        filter_cql: str | dict | None = None,
        order: ItemSort | list[dict[str, str]] = ItemSort.DEFAULT_SORT,
    ) -> Generator[DatasetItem]:
        """
        Search datasets using Explorer's spatial table

        Returned as DatasetItem records, with optional embedded full Datasets
        (if full_dataset==True)

        Returned results are always sorted by (center_time, id)
        """
        field_exprs = self._get_field_exprs(product_names)

        columns = [
            field_exprs["geometry"].label("geometry"),
            field_exprs["bbox"].label("bbox"),
            # TODO: dataset label?
            field_exprs["region_code"].label("region_code"),
            field_exprs["creation_time"],
            field_exprs["datetime"],
            field_exprs["id"].label("id"),
            field_exprs["collection"].label("product_name"),
        ]

        query = self.e_index.spatial_select_query(columns, full=full_dataset)

        # Add all the filters
        query = self._add_fields_to_query(
            query,
            field_exprs,
            product_names=product_names,
            time=time,
            bbox=bbox,
            intersects=intersects,
            dataset_ids=dataset_ids,
        )

        if filter_cql:
            query = self._add_filter_to_query(
                query, field_exprs, filter_lang, filter_cql
            )

        # Maybe sort
        if order == ItemSort.DEFAULT_SORT:
            query = query.order_by(field_exprs["datetime"], field_exprs["id"])
        elif order == ItemSort.UNSORTED:
            ...  # Nothing! great!
        elif order == ItemSort.RECENTLY_ADDED:
            if not full_dataset:
                raise NotImplementedError(
                    "Only full-dataset searches can be sorted by recently added"
                )
            query = query.order_by(self.e_index.ds_added_expr().desc())
        elif order:  # order was provided as a sortby query
            query = self._add_order_to_query(query, field_exprs, order)

        query = query.limit(limit).offset(
            # TODO: Offset/limit isn't particularly efficient for paging...
            offset
        )

        for r in self.e_index.execute_query(query):
            yield DatasetItem(
                dataset_id=r.id,
                bbox=_box2d_to_bbox(r.bbox) if r.bbox else None,
                product_name=r.product_name,
                geometry=(
                    _get_shape(r.geometry, self.e_index.get_srid_name(r.geometry.srid))
                    if r.geometry is not None
                    else None
                ),
                region_code=r.region_code,
                creation_time=r.creation_time,
                center_time=r.center_time,
                odc_dataset=(self.index.datasets.get(r.id) if full_dataset else None),
            )

    def search_collections(
        self,
        name: str | None = None,
        time: tuple[datetime, datetime] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        q: list[str] | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> Generator[CollectionItem]:
        for r in self.e_index.collections_search_query(
            name=name, bbox=bbox, time=time, q=q, limit=limit, offset=offset
        ):
            # the 'r' at the moment has
            # ('definition', 'name', 'bbox', 'time_earliest', 'time_latest')
            yield CollectionItem(
                name=r.name,
                time_earliest=r.time_earliest.astimezone(default_timezone)
                if r.time_earliest
                else None,
                time_latest=r.time_latest.astimezone(default_timezone)
                if r.time_latest
                else None,
                bbox=r.bbox,
                definition=r.definition,
            )

    def _recalculate_period(
        self,
        product: ProductSummary,
        year: int | None,
        month: int | None,
        product_refresh_time: datetime,
    ) -> TimePeriodOverview:
        """Recalculate the given period and store it in the DB"""
        if year and month:
            summary = self._summariser.calculate_summary(
                product.name, year, month, None, product_refresh_time
            )
        elif year:
            summary = TimePeriodOverview.add_periods(
                product.name,
                product_refresh_time,
                (
                    p
                    for month_ in range(1, 13)
                    if (p := self.get(product.name, year, month_, None))
                    and p is not None
                ),
            )

        # Product. Does it have data?
        elif product.dataset_count > 0 and product.duration is not None:
            time_earliest, time_latest = product.duration
            summary = TimePeriodOverview.add_periods(
                product.name,
                product_refresh_time,
                (
                    self.get(product.name, year_, None, None)
                    for year_ in range(
                        time_earliest.astimezone(self.grouping_timezone).year,
                        time_latest.astimezone(self.grouping_timezone).year + 1,
                    )
                ),
            )
        else:
            summary = TimePeriodOverview.empty(product.name, product_refresh_time)
        # FIXME: these should be set inside the methods.
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
        minimum_change_scan_window: timedelta | None = None,
    ) -> tuple[GenerateResult, TimePeriodOverview | None]:
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

        old_product = self.get_product_summary(product_name)

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

        rv = self.refresh_product_extent(
            product_name,
            scan_for_deleted=recreate_dataset_extents or force,
            only_those_newer_than=(
                None if recreate_dataset_extents else only_datasets_newer_than
            ),
        )
        if rv is None:
            return GenerateResult.ERROR, None
        extent_changes, new_product = rv
        assert new_product.id_ is not None
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

            months_to_update: list[tuple[date, str]] | list[tuple[date, int]] = sorted(
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
                new_product, change_month.year, change_month.month, refresh_timestamp
            )

        # Find year records who are older than their month records
        #   (This will find any months calculated above, as well
        #    as from previous interrupted runs.)
        years_to_update = self.find_years_needing_update(product_name)
        for year in years_to_update:
            self._recalculate_period(new_product, year, None, refresh_timestamp)

        # Now update the whole-product record
        updated_summary = self._recalculate_period(
            new_product, None, None, refresh_timestamp
        )
        _LOG.info(
            "product.complete!",
            product_name=new_product.name,
            previous_refresh_time=new_product.last_successful_summary_time,
            new_refresh_time=refresh_timestamp,
        )
        self._mark_product_refresh_completed(new_product.id_, refresh_timestamp)

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

    def _already_summarised_months(self, product_name: str) -> set[date]:
        """Get all months that have a recorded summary already for this product"""

        existing_product = self.get_product_summary(product_name)
        if not existing_product or existing_product.id_ is None:
            return set()

        return {
            r.start_day
            for r in self.e_index.already_summarised_period(
                "month", existing_product.id_
            )
        }

    def _database_time_now(self) -> datetime:
        """
        What's the current time according to the database?

        Any change timestamps stored in the database are using database-local
        time, which could be different to the time on this current machine!
        """
        return self.e_index.execute_query_scalar(select(func.now()))

    def _newest_known_dataset_addition_time(self, product_name: str) -> datetime | None:
        """
        Of all the datasets that are present in Explorer's own tables, when
        was the most recent one indexed to ODC?
        """
        id_ = self.get_product(product_name).id
        return None if id_ is None else self.e_index.latest_dataset_added_time(id_)

    def _mark_product_refresh_completed(
        self, product_id: int, refresh_timestamp: datetime
    ) -> None:
        """
        Mark the product as successfully refreshed at the given product-table timestamp

        (so future runs will be incremental from this point onwards)
        """
        self.e_index.update_product_refresh_timestamp(product_id, refresh_timestamp)
        self._product.cache_clear()

    def list_complete_products(self) -> list[str]:
        """
        List all names of products that have summaries available.
        """
        return sorted(
            product.name
            for product in self.all_products()
            if self.has(product.name, None, None, None)
        )

    def find_datasets_for_region(
        self,
        product: Product,
        region_code: str,
        year: int | None,
        month: int | None,
        day: int | None,
        limit: int,
        offset: int,
    ) -> Generator[Dataset]:
        time_range = _utils.as_time_range(
            year, month, day, tzinfo=self.grouping_timezone
        )
        return self.e_index.datasets_by_region(
            product, region_code, time_range, limit, offset=offset
        )

    def find_products_for_region(
        self,
        region_code: str,
        year: int | None,
        month: int | None,
        day: int | None,
        limit: int,
        offset: int,
    ) -> Iterable[Product]:
        time_range = _utils.as_time_range(
            year, month, day, tzinfo=self.grouping_timezone
        )
        return (
            self._product_by_id(res)
            for res in self.e_index.products_by_region(
                region_code, time_range, limit, offset=offset
            )
        )

    @ttl_cache(ttl=DEFAULT_TTL)
    def _region_summaries(self, product_name: str) -> dict[str, RegionSummary]:
        product = self.get_product(product_name)
        if product.id is None:
            return {}
        return {
            code: RegionSummary(
                product_name=product_name,
                region_code=code,
                count=count,
                generation_time=generation_time,
                footprint_wgs84=to_shape(geom),
            )
            for code, count, generation_time, geom in self.e_index.product_region_summary(
                product.id
            )
            if geom is not None
        }

    def get_product_region_info(self, product_name: str) -> RegionInfo:
        return RegionInfo.for_product(
            product=self.get_product(product_name),
            known_regions=self._region_summaries(product_name),
        )

    def get_dataset_footprint_region(self, dataset_id: UUID):
        """
        Get the recorded WGS84 footprint and region code for a given dataset.

        Note that these will be None if the product has not been summarised.
        """
        rows = self.e_index.dataset_footprint_region(dataset_id).fetchall()
        if not rows:
            return None, None
        row = rows[0]

        footprint = row.footprint
        return (to_shape(footprint) if footprint is not None else None, row.region_code)


def _summary_from_row(res: RowMapping, product_name: str, grouping_timezone: tzinfo):
    timeline_dataset_counts = Counter(
        dict(
            zip(
                res["timeline_dataset_start_days"],
                res["timeline_dataset_counts"],
                strict=True,
            )
        )
        if res["timeline_dataset_start_days"]
        else None
    )
    region_dataset_counts = Counter(
        dict(zip(res["regions"], res["region_dataset_counts"], strict=True))
        if res["regions"]
        else None
    )
    period_type = res["period_type"]
    year, month, day = TimePeriodOverview.from_flat_period_representation(
        period_type, res["start_day"]
    )

    earliest = res["time_earliest"]
    latest = res["time_latest"]
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
                earliest.astimezone(grouping_timezone),
                latest.astimezone(grouping_timezone),
            )
            if earliest and latest
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
            else f"EPSG:{res['footprint_geometry'].srid}"
        ),
        size_bytes=res["size_bytes"],
        footprint_count=res["footprint_count"],
        # The most newly created dataset
        newest_dataset_creation_time=res["newest_dataset_creation_time"],
        product_refresh_time=res["product_refresh_time"],
        # When this summary was last generated
        summary_gen_time=res["generation_time"],
        crses=set(crses) if (crses := res["crses"]) is not None else set(),
    )


def _common_paths_for_uris(
    uri_samples: Iterable[str],
) -> Generator[ProductLocationSample]:
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


def _counter_key_vals(counts: Counter, null_sort_key="ø") -> tuple[tuple, tuple]:
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


_BOX2D_PATTERN = re.compile(
    r"BOX\(([-0-9.e]+)\s+([-0-9.e]+)\s*,\s*([-0-9.e]+)\s+([-0-9.e]+)\)"
)


def _box2d_to_bbox(pg_box2d: str) -> tuple[float, float, float, float]:
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
    return tuple(float(m) for m in m.groups())  # type: ignore[return-value]


def _get_shape(geometry: WKBElement | None, crs: MaybeCRS) -> Geometry | None:
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
        assert math.isclose(shape.area, newshape.area, abs_tol=0.0001), (
            f"{shape.area} != {newshape.area}"
        )
        shape = newshape
    return shape
