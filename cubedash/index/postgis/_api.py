from collections.abc import Generator, Iterable
from datetime import date, datetime, timedelta
from uuid import UUID

import shapely.ops
from cachetools.func import lru_cache
from datacube.drivers.postgis._api import PostgisDbAPI, _dataset_select_fields
from datacube.drivers.postgis._fields import PgDocField
from typing_extensions import override

from datacube.drivers.postgis._schema import (  # isort: skip
    Dataset as ODC_DATASET,  # noqa: N814
    Product as ODC_PRODUCT,  # noqa: N814
)
from datacube.index import Index
from datacube.model import Dataset, MetadataType, Product, Range
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from sqlalchemy import (
    Integer,
    SmallInteger,
    String,
    and_,
    bindparam,
    case,
    cast,
    column,
    delete,
    exists,
    func,
    literal,
    or_,
    select,
    text,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import TSTZRANGE, insert
from sqlalchemy.orm import aliased
from sqlalchemy.sql import ColumnElement
from sqlalchemy.types import TIMESTAMP

import cubedash.summary._schema as _schema
from cubedash._utils import datetime_expression
from cubedash.index.api import EmptyDbError, ExplorerAbstractIndex

from ._schema import (  # isort: skip
    FOOTPRINT_SRID_EXPRESSION,
    DatasetSpatial,
    Product as ProductSpatial,
    Region,
    SpatialQualityStats,
    SpatialRefSys,
    TimeOverview,
    init_elements,
    get_srid_name as srid_name,
)


class ExplorerIndex(ExplorerAbstractIndex):
    name = "pgis_index"

    def __init__(self, index: Index):
        self.index = index
        self.engine = index._db._engine
        self.db_api = PostgisDbAPI

    @override
    def get_mutable_dataset_search_fields(
        self, md: MetadataType
    ) -> dict[str, PgDocField]:
        """
        Get a copy of a metadata type's fields that we can mutate.

        (the ones returned by the Index are cached and so may be shared among callers)
        """
        # why not do md.dataset_fields?
        return self.index._db.get_dataset_fields(md.definition)

    @override
    def ds_added_expr(self):
        return ODC_DATASET.added

    @override
    def get_datasets_derived(
        self, dataset_id: UUID, limit=None
    ) -> tuple[list[Dataset], int]:
        derived_ids = self.index.lineage.get_derived_tree(
            dataset_id, max_depth=1
        ).child_datasets()
        if limit:
            remaining_records = len(derived_ids) - limit
            derived_ids = list(derived_ids)[:limit]
        else:
            remaining_records = 0
        return self.index.datasets.bulk_get(derived_ids), remaining_records

    @override
    def get_dataset_sources(
        self, dataset_id: UUID, limit=None
    ) -> tuple[list[Dataset], int]:
        """
        Get the direct source datasets of a dataset.

        A limit can also be specified.

        Returns a list of sources and how many more sources exist beyond the limit.
        """
        source_ids = self.index.lineage.get_source_tree(
            dataset_id, max_depth=1
        ).child_datasets()
        if limit and len(source_ids) > limit:
            remaining_records = len(source_ids) - limit
            source_ids = list(source_ids)[:limit]
        else:
            remaining_records = 0

        return self.index.datasets.bulk_get(source_ids), remaining_records

    def find_months_needing_update(
        self,
        product_name: str,
        only_those_newer_than: datetime,
    ) -> Iterable[tuple[date, int]]:
        """
        What months have had dataset changes since they were last generated?
        """
        product = self.index.products.get_by_name_unsafe(product_name)

        # Find the most-recently updated datasets and group them by month.
        with self.index._active_connection() as conn:
            return sorted(
                (month.date(), count)  # count isn't even used outside of log.debug
                for month, count in conn.execute(
                    select(
                        func.date_trunc(
                            "month", datetime_expression(product.metadata_type)
                        ).label("month"),
                        func.count(),
                    )
                    .where(
                        and_(
                            ODC_DATASET.product_ref == product.id,
                            ODC_DATASET.updated > only_those_newer_than,
                        )
                    )
                    .group_by("month")
                    .order_by("month")
                )
            )

    @override
    def outdated_months(
        self,
        product: Product,
        only_those_newer_than: datetime,
    ):
        """
        What months have had dataset changes since they were last generated?
        """
        # Find the most-recently updated datasets and group them by month.
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.date_trunc(
                        "month", datetime_expression(product.metadata_type)
                    ).label("month"),
                    func.count(),
                )
                .select_from(ODC_DATASET)
                .where(
                    and_(
                        ODC_DATASET.product_ref == product.id,
                        column("updated") > only_those_newer_than,
                    )
                )
                .group_by("month")
                .order_by("month")
            )

    @override
    def outdated_years(self, product_id: int):
        updated_months = aliased(TimeOverview, name="updated_months")
        years = aliased(TimeOverview, name="years_needing_update")

        with self.index._active_connection() as conn:
            return conn.execute(
                # Select years
                select(years.start_day)
                .where(years.period_type == "year")
                .where(
                    years.product_ref == product_id,
                )
                # Where there exist months that are more newly created.
                .where(
                    exists(
                        select(updated_months.start_day)
                        .where(updated_months.period_type == "month")
                        .where(
                            func.extract("year", updated_months.start_day)
                            == func.extract("year", years.start_day)
                        )
                        .where(
                            updated_months.product_ref == product_id,
                        )
                        .where(updated_months.generation_time > years.generation_time)
                    )
                )
            )

    @override
    def product_ds_count_per_period(self):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    ProductSpatial.name,
                    TimeOverview.start_day,
                    TimeOverview.period_type,
                    TimeOverview.dataset_count,
                )
                .select_from(TimeOverview)
                .join(ProductSpatial)
                .where(TimeOverview.product_ref == ProductSpatial.id)
                .order_by(
                    ProductSpatial.name,
                    TimeOverview.start_day,
                    TimeOverview.period_type,
                )
            )

    @override
    def upsert_product_record(self, product_name: str, fields):
        # Dear future reader. This section used to use an 'UPSERT' statement (as in,
        # insert, on_conflict...) and while this works, it triggers the sequence
        # `product_id_seq` to increment as part of the check for insertion. This
        # is bad because there's only 32 k values in the sequence and we have run out
        # a couple of times! So, It appears that this update-else-insert must be done
        # in two statements...
        with self.index._active_connection(transaction=True) as conn:
            row = conn.execute(
                select(ProductSpatial.id, ProductSpatial.last_refresh).where(
                    ProductSpatial.name == product_name
                )
            ).fetchone()

            if row:
                # Product already exists, so update it
                return conn.execute(
                    update(ProductSpatial)
                    .returning(ProductSpatial.id, ProductSpatial.last_refresh)
                    .where(ProductSpatial.id == row[0])
                    .values(**fields)
                ).fetchone()
            else:
                # Product doesn't exist, so insert it
                fields["name"] = product_name
                return conn.execute(
                    insert(ProductSpatial)
                    .returning(ProductSpatial.id, ProductSpatial.last_refresh)
                    .values(**fields)
                ).fetchone()

    @override
    def put_summary(self, product_id: int, start_day, period, summary_row: dict):
        with self.index._active_connection() as conn:
            return conn.execute(
                insert(TimeOverview)
                .returning(TimeOverview.generation_time)
                .on_conflict_do_update(
                    index_elements=["product_ref", "start_day", "period_type"],
                    set_=summary_row,
                    where=and_(
                        TimeOverview.product_ref == product_id,
                        TimeOverview.start_day == start_day,
                        TimeOverview.period_type == period,
                    ),
                )
                .values(
                    product_ref=product_id,
                    start_day=start_day,
                    period_type=period,
                    **summary_row,
                )
            )

    @override
    def product_summary_cols(self, product_name: str):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    ProductSpatial.dataset_count,
                    ProductSpatial.time_earliest,
                    ProductSpatial.time_latest,
                    ProductSpatial.last_refresh.label("last_refresh_time"),
                    ProductSpatial.last_successful_summary.label(
                        "last_successful_summary_time"
                    ),
                    ProductSpatial.id.label("id_"),
                    ProductSpatial.source_product_refs,
                    ProductSpatial.derived_product_refs,
                    ProductSpatial.fixed_metadata,
                ).where(ProductSpatial.name == product_name)
            ).fetchone()

    @override
    def upsert_product_regions(self, product_id: int):
        # add new regions row and/or update existing regions based on dataset_spatial
        with self.index._active_connection() as conn:
            return conn.execute(
                text(f"""
            with srid_groups as (
                select cubedash.dataset_spatial.product_ref                         as product_ref,
                        cubedash.dataset_spatial.region_code                             as region_code,
                        ST_Transform(ST_Union(cubedash.dataset_spatial.footprint), 4326) as footprint,
                        count(*)                                                         as count
                from cubedash.dataset_spatial
                where cubedash.dataset_spatial.product_ref = {product_id}
                        and
                        st_isvalid(cubedash.dataset_spatial.footprint)
                group by cubedash.dataset_spatial.product_ref,
                        cubedash.dataset_spatial.region_code,
                        st_srid(cubedash.dataset_spatial.footprint)
            )
            insert into cubedash.region (product_ref, region_code, footprint, count)
                select srid_groups.product_ref,
                    coalesce(srid_groups.region_code, '')                          as region_code,
                    ST_SimplifyPreserveTopology(
                            ST_Union(ST_Buffer(srid_groups.footprint, 0)), 0.0001) as footprint,
                    sum(srid_groups.count)                                         as count
                from srid_groups
                group by srid_groups.product_ref, srid_groups.region_code
            on conflict (product_ref, region_code)
                do update set count           = excluded.count,
                            generation_time = now(),
                            footprint       = excluded.footprint
            returning product_ref, region_code, footprint, count

                """)
            )

    @override
    def delete_product_empty_regions(self, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                text(f"""
            delete from cubedash.region
            where product_ref = {product_id} and region_code not in (
                select cubedash.dataset_spatial.region_code
                from cubedash.dataset_spatial
                where cubedash.dataset_spatial.product_ref = {product_id}
                group by cubedash.dataset_spatial.region_code
            )
                """),
            )

    @override
    def product_time_overview(self, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.min(DatasetSpatial.center_time),
                    func.max(DatasetSpatial.center_time),
                    func.count(),
                ).where(DatasetSpatial.product_ref == product_id)
            ).fetchone()

    @override
    def product_time_summary(self, product_id: int, start_day, period):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(TimeOverview).where(
                    and_(
                        TimeOverview.product_ref == product_id,
                        TimeOverview.start_day == start_day,
                        TimeOverview.period_type == period,
                    )
                )
            )

    @override
    def latest_arrivals(self, period_length: timedelta):
        with self.engine.begin() as conn:
            latest_arrival_date: datetime = conn.execute(
                text("select max(added) from odc.dataset;")
            ).scalar()
            if latest_arrival_date is None:
                raise EmptyDbError()

            datasets_since_date = (latest_arrival_date - period_length).date()

            # shouldn't this be getting from odc.dataset combined with dataset_spatial?
            # no point returning datasets that have been added in the odc database but not the cubedash one
            return conn.execute(
                text("""
                    select
                    date_trunc('day', added) as arrival_date,
                    (select name from odc.product where id = d.product_ref) product_name,
                    count(*),
                    (array_agg(id))[0:3]
                    from odc.dataset d
                    where d.added > :datasets_since
                    group by arrival_date, product_name
                    order by arrival_date desc, product_name;
                """),
                {
                    "datasets_since": datasets_since_date,
                },
            )

    @override
    def already_summarised_period(self, period: str, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(TimeOverview.start_day).where(
                    and_(
                        TimeOverview.product_ref == product_id,
                        TimeOverview.period_type == period,
                    )
                )
            )

    @override
    def linked_products_search(self, product_id: int, sample_sql: str, direction: str):
        from_ref, to_ref = "source_dataset_ref", "derived_dataset_ref"
        if direction == "derived":
            to_ref, from_ref = from_ref, to_ref

        with self.index._active_connection() as conn:
            return conn.execute(
                text(f"""
                with datasets as (
                    select id from odc.dataset {sample_sql}
                    where product_ref={product_id}
                    and archived is null
                ),
                linked_datasets as (
                    select distinct {from_ref} as linked_dataset_ref
                    from odc.dataset_lineage
                    inner join datasets d on d.id = {to_ref}
                ),
                linked_products as (
                    select distinct product_ref
                    from odc.dataset
                    inner join linked_datasets on id = linked_dataset_ref
                    where archived is null
                )
                select array_agg(name order by name)
                from odc.product
                inner join linked_products sp on id = product_ref;
            """)
            )

    @override
    def product_region_summary(self, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    Region.region_code,
                    Region.count,
                    Region.generation_time,
                    Region.footprint,
                )
                .where(Region.product_ref == product_id)
                .order_by(Region.region_code)
            )

    @override
    def dataset_footprint_region(self, dataset_id):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.ST_Transform(DatasetSpatial.footprint, 4326).label(
                        "footprint"
                    ),
                    DatasetSpatial.region_code,
                ).where(DatasetSpatial.id == dataset_id)
            )

    @override
    def latest_dataset_added_time(self, product_id: int):
        # DATASET_SPATIAL doesn't keep track of when the dataset was indexed,
        # so we have to get that info from ODC_DATASET
        # join might not be necessary
        with self.index._active_connection() as conn:
            return conn.execute(
                select(func.max(ODC_DATASET.added))
                .select_from(DatasetSpatial)
                .join(ODC_DATASET, onclause=DatasetSpatial.id == ODC_DATASET.id)
                .where(DatasetSpatial.product_ref == product_id)
            ).scalar()

    @override
    def update_product_refresh_timestamp(
        self, product_id: int, refresh_timestamp: datetime
    ):
        with self.index._active_connection() as conn:
            return conn.execute(
                update(ProductSpatial)
                .where(ProductSpatial.id == product_id)
                .where(
                    or_(
                        ProductSpatial.last_successful_summary.is_(None),
                        ProductSpatial.last_successful_summary
                        < refresh_timestamp.isoformat(),
                    )
                )
                .values(last_successful_summary=refresh_timestamp)
            )

    # does this add much value? and if so, is there a better way to do it?
    @override
    def find_fixed_columns(self, field_values, candidate_fields, sample_ids):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    *[
                        (
                            func.every(
                                field.alchemy_expression == field_values[field_name]
                            )
                        ).label(field_name)
                        if field.type_name != "datetime"
                        else (
                            func.every(
                                cast(field.alchemy_expression, TIMESTAMP(timezone=True))
                                == field_values[field_name]
                            )
                        ).label(field_name)
                        for field_name, field in candidate_fields
                    ]
                )
                .select_from(ODC_DATASET)
                .where(ODC_DATASET.id.in_([r for (r,) in sample_ids]))
            )

    # does this really add much value? and if so, is there a better way to do it?
    @override
    def all_products_location_samples(
        self, products: list[Product], sample_size: int = 50
    ):
        queries = []
        for product in products:
            subquery = (
                select(
                    literal(product.name).label("name"),
                    func.array_agg(ODC_DATASET.uri).label("uris"),
                )
                .where(
                    and_(
                        ODC_DATASET.product_ref == product.id,
                        ODC_DATASET.archived.is_(None),
                    )
                )
                .limit(sample_size)
            )
            queries.append(subquery)

        if queries:  # Don't run invalid SQL on empty database
            with self.index._active_connection() as conn:
                return conn.execute(union_all(*queries))
        else:
            raise EmptyDbError()

    # This is tied to ODC's internal Dataset search implementation as there's no higher-level api to allow this.
    # When region_code is integrated into core (as is being discussed) this can be replaced.
    # pylint: disable=protected-access
    @override
    def datasets_by_region(
        self,
        product: Product,
        region_code: str,
        time_range: Range,
        limit: int,
        offset: int = 0,
    ) -> Generator[Dataset, None, None]:
        query = (
            select(*_dataset_select_fields())
            .select_from(DatasetSpatial)
            .join(ODC_DATASET, DatasetSpatial.id == ODC_DATASET.id)
            .where(DatasetSpatial.region_code == bindparam("region_code", region_code))
            .where(DatasetSpatial.product_ref == bindparam("product_ref", product.id))
        )
        if time_range:
            query = query.where(
                DatasetSpatial.center_time > bindparam("from_time", time_range.begin)
            ).where(DatasetSpatial.center_time < bindparam("to_time", time_range.end))
        query = (
            query.order_by(DatasetSpatial.center_time.desc())
            .limit(bindparam("limit", limit))
            .offset(bindparam("offset", offset))
        )
        with self.index._active_connection() as conn:
            return (
                self.index.datasets._make(res, full_info=True)
                for res in conn.execute(query).fetchall()
            )

    @override
    def products_by_region(
        self,
        region_code: str,
        time_range: Range,
        limit: int,
        offset: int = 0,
    ) -> Generator[int, None, None]:
        query = (
            select(DatasetSpatial.product_ref)
            .distinct()
            .where(DatasetSpatial.region_code == bindparam("region_code", region_code))
        )
        if time_range:
            query = query.where(
                DatasetSpatial.center_time > bindparam("from_time", time_range.begin)
            ).where(DatasetSpatial.center_time < bindparam("to_time", time_range.end))

        query = (
            query.order_by(DatasetSpatial.product_ref)
            .limit(bindparam("limit", limit))
            .offset(bindparam("offset", offset))
        )
        with self.index._active_connection() as conn:
            return (res.product_ref for res in conn.execute(query).fetchall())

    @override
    def delete_datasets(
        self, product_id: int, after_date: datetime = None, full: bool = False
    ):
        with self.index._active_connection() as conn:
            # Forcing? Check every other dataset for removal, so we catch manually-deleted rows from the table.
            if full:
                return conn.execute(
                    delete(DatasetSpatial)
                    .where(
                        DatasetSpatial.product_ref == product_id,
                    )
                    .where(
                        ~DatasetSpatial.id.in_(
                            select(ODC_DATASET.id).where(
                                ODC_DATASET.product_ref == product_id,
                            )
                        )
                    )
                ).rowcount

            # Remove any archived datasets from our spatial table.
            archived_datasets = (
                select(ODC_DATASET.id)
                .select_from(ODC_DATASET)
                .where(
                    and_(
                        ODC_DATASET.archived.isnot(None),
                        ODC_DATASET.product_ref == product_id,
                    )
                )
            )
            if after_date is not None:
                archived_datasets = archived_datasets.where(
                    ODC_DATASET.updated
                    > after_date  # updated should also capture added time
                )

            return conn.execute(
                delete(DatasetSpatial).where(DatasetSpatial.id.in_(archived_datasets))
            ).rowcount

    @override
    def upsert_datasets(self, product_id, column_values, after_date):
        column_values["id"] = ODC_DATASET.id
        column_values["product_ref"] = ODC_DATASET.product_ref
        only_where = [
            ODC_DATASET.product_ref
            == bindparam("product_ref", product_id, type_=SmallInteger),
            ODC_DATASET.archived.is_(None),
        ]
        if after_date is not None:
            only_where.append(
                ODC_DATASET.updated
                > after_date  # updated should also capture added time
            )
        with self.index._active_connection() as conn:
            stmt = insert(DatasetSpatial).from_select(
                list(column_values.keys()),
                select(*column_values.values()).where(and_(*only_where)),
            )
            return conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_=stmt.excluded,
                )
            ).rowcount

    @override
    def synthesize_dataset_footprint(self, rows, shapes):
        with self.engine.begin() as conn:
            return conn.execute(
                update(DatasetSpatial)
                .where(DatasetSpatial.id == bindparam("dataset_id"))
                .values(footprint=bindparam("footprint")),
                [
                    dict(
                        dataset_id=id_,
                        footprint=from_shape(
                            shapely.ops.unary_union(
                                [
                                    shapes[(int(sat_path.lower), row)]
                                    for row in range(
                                        int(sat_row.lower),
                                        int(sat_row.upper) + 1,
                                    )
                                ]
                            ),
                            srid=4326,
                            extended=True,
                        ),
                    )
                    for id_, sat_path, sat_row in rows
                ],
            )

    @override
    def dataset_spatial_field_exprs(self):
        geom = func.ST_Transform(DatasetSpatial.footprint, 4326)
        field_exprs = dict(
            collection=(
                select(ODC_PRODUCT.name)
                .where(ODC_PRODUCT.id == DatasetSpatial.product_ref)
                .scalar_subquery()
            ),
            datetime=DatasetSpatial.center_time,
            creation_time=DatasetSpatial.creation_time,
            geometry=geom,
            bbox=func.Box2D(geom).cast(String),
            region_code=DatasetSpatial.region_code,
            id=DatasetSpatial.id,
        )
        return field_exprs

    @override
    def spatial_select_query(self, clauses, full: bool = False):
        query = select(*clauses)
        if full:
            return query.select_from(DatasetSpatial).join(
                ODC_DATASET, onclause=ODC_DATASET.id == DatasetSpatial.id
            )
        return query.select_from(DatasetSpatial)

    @override
    def select_spatial_stats(self):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    SpatialQualityStats.product_ref.label("product_ref"),
                    SpatialQualityStats.count,
                    SpatialQualityStats.missing_footprint,
                    SpatialQualityStats.footprint_size,
                    SpatialQualityStats.footprint_stddev,
                    SpatialQualityStats.missing_srid,
                    SpatialQualityStats.has_file_size,
                    SpatialQualityStats.has_region,
                )
            )

    @override
    def schema_initialised(self) -> bool:
        """
        Do our DB schemas exist?
        """
        with self.engine.begin() as conn:
            return _schema.has_schema(conn)

    @override
    def schema_compatible_info(self, for_writing_operations_too=False):
        """
        Schema compatibility information
        postgis version, if schema has latest changes (optional: and has updated column)
        """
        with self.engine.begin() as conn:
            return (
                _schema.get_postgis_versions(conn),
                _schema.is_compatible_schema(
                    conn,
                    "odc.dataset",  # is there an ODC_DATASET.fullname equivalent?
                    for_writing_operations_too,
                ),
            )

    @override
    def init_schema(self, grouping_epsg_code: int):
        with self.engine.begin() as conn:
            return init_elements(conn, grouping_epsg_code)

    @override
    def refresh_stats(self, concurrently=False):
        """
        Refresh general statistics tables that cover all products.

        This is ideally done once after all needed products have been refreshed.
        """
        with self.engine.begin() as conn:
            _schema.refresh_supporting_views(conn, concurrently=concurrently)

    @lru_cache()
    @override
    def get_srid_name(self, srid: int):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        with self.engine.begin() as conn:
            return srid_name(conn, srid)

    @override
    def summary_where_clause(
        self, product_name: str, begin_time: datetime, end_time: datetime
    ) -> ColumnElement:
        return and_(
            func.tstzrange(begin_time, end_time, "[]", type_=TSTZRANGE).contains(
                DatasetSpatial.center_time
            ),
            DatasetSpatial.product_ref
            == (
                select(ODC_PRODUCT.id).where(ODC_PRODUCT.name == product_name)
            ).scalar_subquery(),
            or_(
                func.st_isvalid(DatasetSpatial.footprint).is_(True),
                func.st_isvalid(DatasetSpatial.footprint).is_(None),
            ),
        )

    @override
    def srid_summary(self, where_clause: ColumnElement):
        select_by_srid = (
            select(
                func.ST_SRID(DatasetSpatial.footprint).label("srid"),
                func.count().label("dataset_count"),
                func.ST_Transform(
                    func.ST_Union(DatasetSpatial.footprint),
                    FOOTPRINT_SRID_EXPRESSION,
                    type_=Geometry(),
                ).label("footprint_geometry"),
                func.sum(DatasetSpatial.size_bytes).label("size_bytes"),
                func.max(DatasetSpatial.creation_time).label(
                    "newest_dataset_creation_time"
                ),
            )
            .where(where_clause)
            .group_by("srid")
            .alias("srid_summaries")
        )

        # Union all srid groups into one summary.
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    # do we still need .c. notation here?
                    func.sum(select_by_srid.c.dataset_count).label("dataset_count"),
                    func.array_agg(select_by_srid.c.srid).label("srids"),
                    func.sum(select_by_srid.c.size_bytes).label("size_bytes"),
                    func.ST_Union(
                        func.ST_Buffer(select_by_srid.c.footprint_geometry, 0),
                        type_=Geometry(),
                    ).label("footprint_geometry"),
                    func.max(select_by_srid.c.newest_dataset_creation_time).label(
                        "newest_dataset_creation_time"
                    ),
                    func.now().label("summary_gen_time"),
                )
            )

    @override
    def day_counts(self, grouping_time_zone, where_clause: ColumnElement):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.date_trunc(
                        "day",
                        DatasetSpatial.center_time.op("AT TIME ZONE")(
                            grouping_time_zone
                        ),
                    ).label("day"),
                    func.count(),
                )
                .where(where_clause)
                .group_by("day")
            )

    @override
    def region_counts(self, where_clause):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    DatasetSpatial.region_code.label("region_code"),
                    func.count(),
                )
                .where(where_clause)
                .group_by("region_code")
            )

    @override
    def ds_srid_expression(self, spatial_ref, projection, default_crs: str = None):
        default_crs_expression = None
        if default_crs:
            auth_name, auth_srid = default_crs.split(":")
            default_crs_expression = (
                select(SpatialRefSys.srid)
                .where(func.lower(SpatialRefSys.auth_name) == auth_name.lower())
                .where(SpatialRefSys.auth_srid == int(auth_srid))
                .scalar_subquery()
            )
        return func.coalesce(
            case(
                (
                    # If matches shorthand code: eg. "epsg:1234"
                    spatial_ref.op("~")(r"^[A-Za-z0-9]+:[0-9]+$"),
                    select(SpatialRefSys.srid)
                    .where(
                        func.lower(SpatialRefSys.auth_name)
                        == func.lower(func.split_part(spatial_ref, ":", 1))
                    )
                    .where(
                        SpatialRefSys.auth_srid
                        == func.split_part(spatial_ref, ":", 2).cast(Integer)
                    )
                    .scalar_subquery(),
                ),
                else_=None,
            ),
            case(
                (
                    # Plain WKT that ends in an authority code.
                    # Extract the authority name and code using regexp. Yuck!
                    # Eg: ".... AUTHORITY["EPSG","32756"]]"
                    spatial_ref.op("~")(r'AUTHORITY\["[a-zA-Z0-9]+", *"[0-9]+"\]\]$'),
                    select(SpatialRefSys.srid)
                    .where(
                        func.lower(SpatialRefSys.auth_name)
                        == func.lower(
                            func.substring(
                                spatial_ref,
                                r'AUTHORITY\["([a-zA-Z0-9]+)", *"[0-9]+"\]\]$',
                            )
                        )
                    )
                    .where(
                        SpatialRefSys.auth_srid
                        == func.substring(
                            spatial_ref, r'AUTHORITY\["[a-zA-Z0-9]+", *"([0-9]+)"\]\]$'
                        ).cast(Integer)
                    )
                    .scalar_subquery(),
                ),
                else_=None,
            ),
            # Some older datasets have datum/zone fields instead.
            # The only remaining ones in DEA are 'GDA94'.
            # Is this still relevant for postgis?
            case(
                (
                    projection["datum"].astext == "GDA94",
                    select(SpatialRefSys.srid)
                    .where(func.lower(SpatialRefSys.auth_name) == "epsg")
                    .where(
                        SpatialRefSys.auth_srid
                        == (
                            "283" + func.abs(projection["zone"].astext.cast(Integer))
                        ).cast(Integer)
                    )
                    .scalar_subquery(),
                ),
                else_=None,
            ),
            default_crs_expression,
            # TODO: Handle arbitrary WKT strings (?)
            # 'GEOGCS[\\"GEOCENTRIC DATUM of AUSTRALIA\\",DATUM[\\"GDA94\\",SPHEROID[
            #    \\"GRS80\\",6378137,298.257222101]],PRIMEM[\\"Greenwich\\",0],UNIT[\\
            # "degree\\",0.0174532925199433]]'
        )

    @override
    def sample_dataset(self, product_id: int, columns):
        with self.index._active_connection() as conn:
            res = conn.execute(
                select(
                    ODC_DATASET.id,
                    ODC_DATASET.product_ref,
                    *columns,
                )
                .where(
                    and_(
                        ODC_DATASET.product_ref
                        == bindparam("product_ref", product_id, type_=SmallInteger),
                        ODC_DATASET.archived.is_(None),
                    )
                )
                .limit(1)
            )
            # at this point can we not select the values from DATASET_SPATIAL,
            # or is there a reason we need them to be calculated?
            return res

    @override
    def mapped_crses(self, product, srid_expression):
        with self.index._active_connection() as conn:
            # SQLAlchemy queries require "column == None", not "column is None" due to operator overloading:
            # pylint: disable=singleton-comparison
            res = conn.execute(
                select(
                    literal(product.name).label("product"),
                    srid_expression,
                )
                .where(ODC_DATASET.product_ref == product.id)
                .where(ODC_DATASET.archived.is_(None))
                .limit(1)
            )
            return res
