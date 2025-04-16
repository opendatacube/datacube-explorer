from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
from datetime import datetime, timedelta
from uuid import UUID

from datacube.index import Index
from datacube.model import Dataset, MetadataType, Product, Range
from sqlalchemy.sql import ColumnElement


class EmptyDbError(Exception):
    pass


class ExplorerAbstractIndex(ABC):
    name: str = ""
    index: Index = None
    engine = None

    # need to add an odc_index accessor
    def execute_query(self, query):
        with self.engine.begin() as conn:
            return conn.execute(query)

    def make_dataset(self, row):
        # pylint: disable=protected-access
        return self.index.datasets._make(row, full_info=True)

    def ds_search_returning(
        self,
        fields: Iterable[str] | None = None,
        limit: int | None = None,
        order_by=None,
        args={},
    ):
        # keeping since it's used in _extents without direct access to index but perhaps should remove
        return self.index.datasets.search_returning(
            field_names=fields, limit=limit, order_by=order_by, **args
        )

    @abstractmethod
    def ds_added_expr(self): ...

    @abstractmethod
    def get_mutable_dataset_search_fields(self, md: MetadataType): ...

    @abstractmethod
    def get_datasets_derived(
        self, dataset_id: UUID, limit: int | None = None
    ) -> tuple[list[Dataset], int]: ...

    @abstractmethod
    def get_dataset_sources(
        self, dataset_id: UUID, limit: int | None = None
    ) -> tuple[list[Dataset], int]: ...

    @abstractmethod
    def dataset_footprint_region(self, dataset_id): ...

    @abstractmethod
    def dataset_spatial_field_exprs(self): ...

    @abstractmethod
    def delete_datasets(
        self, product_id: int, after_date: datetime | None = None, full: bool = False
    ) -> int: ...

    @abstractmethod
    def upsert_datasets(self, product_id, column_values, after_date) -> int: ...

    @abstractmethod
    def synthesize_dataset_footprint(self, rows, shapes): ...

    @abstractmethod
    def product_ds_count_per_period(self): ...

    @abstractmethod
    def latest_arrivals(self, period_length: timedelta): ...

    @abstractmethod
    def latest_dataset_added_time(self, product_id: int): ...

    @abstractmethod
    def outdated_months(self, product: Product, only_those_newer_than: datetime): ...

    @abstractmethod
    def outdated_years(self, product_id: int): ...

    @abstractmethod
    def already_summarised_period(self, period: str, product_id: int): ...

    @abstractmethod
    def product_time_overview(self, product_id: int): ...

    @abstractmethod
    def product_time_summary(self, product_id: int, start_day, period): ...

    @abstractmethod
    def put_summary(self, product_id: int, start_day, period, summary_row: dict): ...

    @abstractmethod
    def product_summary_cols(self, product_name: str): ...

    @abstractmethod
    def upsert_product_record(self, product_name: str, fields): ...

    @abstractmethod
    def upsert_product_regions(self, product_id: int): ...

    @abstractmethod
    def delete_product_empty_regions(self, product_id: int): ...

    @abstractmethod
    def product_region_summary(self, product_id: int): ...

    @abstractmethod
    def update_product_refresh_timestamp(
        self, product_id: int, refresh_timestamp: datetime
    ): ...

    @abstractmethod
    def find_fixed_columns(self, field_values, candidate_fields, sample_ids): ...

    @abstractmethod
    def linked_products_search(
        self, product_id: int, sample_sql: str, direction: str
    ): ...

    @abstractmethod
    def all_products_location_samples(
        self, products: list[Product], sample_size: int = 50
    ): ...

    @abstractmethod
    def datasets_by_region(
        self,
        product: Product,
        region_code: str,
        time_range: Range,
        limit: int,
        offset: int = 0,
    ) -> Generator[Dataset, None, None]: ...

    @abstractmethod
    def products_by_region(
        self, region_code: str, time_range: Range, limit: int, offset: int = 0
    ) -> Generator[int, None, None]: ...

    @abstractmethod
    def spatial_select_query(self, clauses, full: bool = False): ...

    @abstractmethod
    def select_spatial_stats(self): ...

    @abstractmethod
    def schema_initialised(self) -> bool: ...

    @abstractmethod
    def schema_compatible_info(
        self, for_writing_operations_too=False
    ) -> tuple[str, bool]: ...

    @abstractmethod
    def init_schema(self, grouping_epsg_code: int): ...

    @abstractmethod
    def refresh_stats(self, concurrently=False): ...

    @abstractmethod
    def get_srid_name(self, srid: int) -> str | None: ...

    @abstractmethod
    def summary_where_clause(
        self, product_name: str, begin_time: datetime, end_time: datetime
    ) -> ColumnElement: ...

    @abstractmethod
    def srid_summary(self, where_clause: ColumnElement): ...

    @abstractmethod
    def day_counts(self, grouping_time_zone, where_clause: ColumnElement): ...

    @abstractmethod
    def region_counts(self, where_clause): ...

    @abstractmethod
    def ds_srid_expression(
        self, spatial_ref, projection, default_crs: str | None = None
    ): ...

    @abstractmethod
    def sample_dataset(self, product_id: int, columns): ...

    @abstractmethod
    def mapped_crses(self, product: Product, srid_expression): ...
