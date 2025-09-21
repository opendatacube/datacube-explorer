from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID

from datacube.index import Index
from datacube.model import Dataset, MetadataType, Product, Range
from datacube.model.fields import Field
from sqlalchemy import CursorResult, Result, Row, Select, inspect
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.elements import ClauseElement, Label

from cubedash.summary._schema import CUBEDASH_SCHEMA, PleaseRefresh


class EmptyDbError(Exception):
    pass


class ExplorerAbstractIndex(ABC):
    def __init__(self, name: str, index: Index) -> None:
        self.name = name
        self.index = index
        # There's no public api for sharing the existing engine (it's an implementation detail of the current index).
        # We could create our own from config, but there's no api for getting the ODC config for the index either.
        # could use: PostgresDb.from_config(index.environment, validate_connection=False)._engine
        # but either approach involves accessing a protected attribute - which is better?
        self.engine = index._db._engine  # type: ignore[attr-defined]

    # need to add an odc_index accessor
    def execute_query(self, query) -> list[Row]:
        with self.engine.connect() as conn:
            return conn.execute(query).fetchall()

    def execute_query_scalar(self, query) -> Any:
        with self.engine.connect() as conn:
            return conn.execute(query).scalar()

    def execute_ddl(self, query) -> int:
        with self.engine.begin() as conn:
            results = conn.execute(query)
            return results.rowcount

    def make_dataset(self, row):
        # pylint: disable=protected-access
        return self.index.datasets._make(row, full_info=True)  # type: ignore[attr-defined]

    def ds_search_returning(
        self,
        fields: Iterable[str] | None = None,
        limit: int | None = None,
        order_by=None,
        args=None,
    ):
        if args is None:
            args = {}
        # keeping since it's used in _extents without direct access to index but perhaps should remove
        return self.index.datasets.search_returning(
            field_names=fields, limit=limit, order_by=order_by, **args
        )

    @abstractmethod
    def ds_added_expr(self) -> ColumnElement: ...

    @abstractmethod
    def get_mutable_dataset_search_fields(
        self, md: MetadataType
    ) -> dict[str, Field]: ...

    @abstractmethod
    def get_datasets_derived(
        self, dataset_id: UUID, limit: int | None = None
    ) -> tuple[Iterable[Dataset], int]: ...

    @abstractmethod
    def get_dataset_sources(
        self, dataset_id: UUID, limit: int | None = None
    ) -> tuple[Iterable[Dataset], int]: ...

    @abstractmethod
    def dataset_footprint_region(self, dataset_id: UUID) -> Result: ...

    @abstractmethod
    def dataset_spatial_field_exprs(self) -> dict[str, ColumnElement]: ...

    @abstractmethod
    def delete_datasets(
        self, product_id: int, after_date: datetime | None = None, full: bool = False
    ) -> int: ...

    @abstractmethod
    def upsert_datasets(
        self,
        product_id: int,
        column_values: dict[str, Label],
        after_date: datetime | None,
    ) -> int: ...

    @abstractmethod
    def synthesize_dataset_footprint(
        self, rows: Sequence[tuple], shapes: dict
    ) -> Result: ...

    @abstractmethod
    def product_ds_count_per_period(self) -> Result: ...

    @abstractmethod
    def latest_arrivals(self, period_length: timedelta) -> Result: ...

    @abstractmethod
    def latest_dataset_added_time(self, product_id: int) -> datetime: ...

    @abstractmethod
    def outdated_months(
        self, product: Product, only_those_newer_than: datetime
    ) -> Result: ...

    @abstractmethod
    def outdated_years(self, product_id: int) -> Result: ...

    @abstractmethod
    def already_summarised_period(self, period: str, product_id: int) -> Result: ...

    @abstractmethod
    def product_time_overview(
        self, product_id: int
    ) -> tuple[datetime, datetime, int]: ...

    @abstractmethod
    def product_time_summary(
        self, product_id: int, start_day: date, period: str
    ) -> Result: ...

    @abstractmethod
    def put_summary(
        self, product_id: int, start_day: date, period: str, summary_row: dict
    ) -> Result: ...

    @abstractmethod
    def product_summary_cols(self, product_name: str) -> Row: ...

    @abstractmethod
    def collections_search_query(
        self,
        limit: int,
        offset: int,
        name: str | None,
        bbox: tuple[float, float, float, float] | None,
        time: tuple[datetime, datetime] | None,
        q: Sequence[str] | None,
    ) -> Result: ...

    @abstractmethod
    def upsert_product_record(
        self, product_name: str, fields: dict[str, Any]
    ) -> tuple[int, datetime]: ...

    @abstractmethod
    def upsert_product_regions(self, product_id: int) -> CursorResult: ...

    @abstractmethod
    def delete_product_empty_regions(self, product_id: int) -> CursorResult: ...

    @abstractmethod
    def product_region_summary(self, product_id: int) -> Result: ...

    @abstractmethod
    def update_product_refresh_timestamp(
        self, product_id: int, refresh_timestamp: datetime
    ) -> Result: ...

    @abstractmethod
    def find_fixed_columns(
        self,
        field_values: dict,
        candidate_fields: Sequence[tuple[str, Field]],
        sample_ids: Iterable[tuple],
    ) -> Result: ...

    @abstractmethod
    def linked_products_search(
        self, product_id: int, sample_sql: str, direction: str
    ) -> Result: ...

    @abstractmethod
    def all_products_location_samples(
        self, products: Sequence[Product], sample_size: int
    ) -> Result: ...

    @abstractmethod
    def datasets_by_region(
        self,
        product: Product,
        region_code: str,
        time_range: Range | None,
        limit: int,
        offset: int = 0,
    ) -> Generator[Dataset]: ...

    @abstractmethod
    def products_by_region(
        self, region_code: str, time_range: Range | None, limit: int, offset: int = 0
    ) -> Generator[int]: ...

    @abstractmethod
    def spatial_select_query(
        self, clauses: Sequence[Label | ClauseElement], full: bool = False
    ) -> Select: ...

    @abstractmethod
    def select_spatial_stats(self) -> Result: ...

    def schema_initialised(self) -> bool:
        return inspect(self.engine).has_schema(CUBEDASH_SCHEMA)

    @abstractmethod
    def schema_compatible_info(
        self, for_writing_operations_too: bool = False
    ) -> tuple[str, bool]: ...

    @abstractmethod
    def init_schema(self, grouping_epsg_code: int) -> set[PleaseRefresh]: ...

    @abstractmethod
    def refresh_stats(self, concurrently: bool) -> None: ...

    @abstractmethod
    def get_srid_name(self, srid: int) -> str | None: ...

    @abstractmethod
    def summary_where_clause(
        self, product_name: str, begin_time: datetime, end_time: datetime
    ) -> ColumnElement: ...

    @abstractmethod
    def srid_summary(self, where_clause: ColumnElement) -> Result: ...

    @abstractmethod
    def day_counts(
        self, grouping_time_zone: str, where_clause: ColumnElement
    ) -> Result: ...

    @abstractmethod
    def region_counts(self, where_clause: ColumnElement) -> Result: ...

    @abstractmethod
    def ds_srid_expression(
        self,
        spatial_ref: str,
        projection: ColumnElement,
        default_crs: str | None = None,
    ) -> ClauseElement: ...

    @abstractmethod
    def sample_dataset(self, product_id: int, columns: Sequence[Label]) -> Result: ...

    @abstractmethod
    def mapped_crses(self, product: Product, srid_expression: Label) -> Result: ...
