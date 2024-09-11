from datetime import datetime, timedelta
from typing import (
    Generator,
)
from uuid import UUID

import shapely.ops
from cachetools.func import lru_cache
from datacube.drivers.postgres._api import _DATASET_SELECT_FIELDS, PostgresDbAPI
from datacube.drivers.postgres._fields import PgDocField
from datacube.drivers.postgres._schema import (
    DATASET as ODC_DATASET,
)
from datacube.drivers.postgres._schema import (
    DATASET_LOCATION,
    DATASET_SOURCE,
)
from datacube.drivers.postgres._schema import (
    PRODUCT as ODC_PRODUCT,
)
from datacube.index import Index
from datacube.model import Dataset, MetadataType, Product, Range
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from sqlalchemy import (
    SmallInteger,
    String,
    and_,
    bindparam,
    exists,
    func,
    literal,
    or_,
    select,
    text,
    union_all,
)
from sqlalchemy.dialects.postgresql import TSTZRANGE, insert
from sqlalchemy.sql import ColumnElement

import cubedash.summary._schema as _schema  # this would be the postgres version of the schema
from cubedash._utils import datetime_expression
from cubedash.index.api import EmptyDbError, ExplorerAbstractIndex
from cubedash.summary._schema import (
    DATASET_SPATIAL,
    FOOTPRINT_SRID_EXPRESSION,
    PRODUCT,
    REGION,
    SPATIAL_QUALITY_STATS,
    TIME_OVERVIEW,
)


class ExplorerIndex(ExplorerAbstractIndex):
    name = "postgres"

    def __init__(self, index: Index):
        self.index = index
        self.engine = (
            index._db._engine
        )  # PostgresDb.from_config(index.environment)._engine
        self.db_api = PostgresDbAPI

    def get_mutable_dataset_search_fields(
        self, md: MetadataType
    ) -> dict[str, PgDocField]:
        """
        Get a copy of a metadata type's fields that we can mutate.

        (the ones returned by the Index are cached and so may be shared among callers)
        """
        # why not do md.dataset_fields?
        return self.index._db.get_dataset_fields(md.definition)

    def ds_added_expr(self):
        # what's the best approach with this one?
        return ODC_DATASET.c.added

    def get_dataset_sources(
        self, dataset_id: UUID, limit=None
    ) -> tuple[dict[str, Dataset], int]:
        """
        Get the direct source datasets of a dataset, but without loading the whole upper provenance tree.

        This is a lighter alternative to doing `index.datasets.get(include_source=True)`

        A limit can also be specified.

        Returns a source dict and how many more sources exist beyond the limit.
        """
        query = select(
            DATASET_SOURCE.c.source_dataset_ref, DATASET_SOURCE.c.classifier
        ).where(DATASET_SOURCE.c.dataset_ref == dataset_id)
        if limit:
            # We add one to detect if there are more records after out limit.
            query = query.limit(limit + 1)

        # engine = alchemy_engine(index)
        with self.index._active_connection() as conn:
            dataset_classifier = conn.execute(query).fetchall()

            if not dataset_classifier:
                return {}, 0

            remaining_records = 0
            if limit and len(dataset_classifier) > limit:
                dataset_classifier = dataset_classifier[:limit]
                remaining_records = (
                    conn.execute(
                        select(func.count())
                        .select_from(DATASET_SOURCE)
                        .where(DATASET_SOURCE.c.dataset_ref == dataset_id)
                    ).scalar()
                    - limit
                )

        classifier = dict(dataset_classifier)
        return {
            classifier[d.id]: d
            for d in (
                self.index.datasets.bulk_get(
                    dataset_id for dataset_id, _ in dataset_classifier
                )
            )
        }, remaining_records

    # Same as PostgresDbApi.get_derived_datasets but with limit
    # not implemented in pgis- what has it been replaced by? presumably something with lineage
    # load_lineage_relations can be used to get derived, but that doesn't return a Dataset, would need an extra step
    # of searching by id. Also doesn't permit limit, although given it's only returning UUIDs, limit may not need
    # to be implemented as part of the query.
    # get_dataset_sources could similarly be replaced with load_lineage_relations
    # Neither can be replaced by Postgres API, however
    def get_datasets_derived(
        self, dataset_id: UUID, limit=None
    ) -> tuple[list[Dataset], int]:
        """
        this is similar to ODC's connection.get_derived_datasets() but allows a
        limit, and will return a total count.
        """

        query = (
            select(*_DATASET_SELECT_FIELDS)
            .select_from(
                ODC_DATASET.join(
                    DATASET_SOURCE, ODC_DATASET.c.id == DATASET_SOURCE.c.dataset_ref
                )
            )
            .where(DATASET_SOURCE.c.source_dataset_ref == dataset_id)
        )
        if limit:
            # We add one to detect if there are more records after out limit.
            query = query.limit(limit + 1)

        with self.index._active_connection() as conn:
            remaining_records = 0
            total_count = 0
            datasets = conn.execute(query).fetchall()

            if limit and len(datasets) > limit:
                datasets = datasets[:limit]
                total_count = conn.execute(
                    select(func.count())
                    .select_from(
                        ODC_DATASET.join(
                            DATASET_SOURCE,
                            ODC_DATASET.c.id == DATASET_SOURCE.c.dataset_ref,
                        )
                    )
                    .where(DATASET_SOURCE.c.source_dataset_ref == dataset_id)
                ).scalar()
                remaining_records = total_count - limit

        return [
            self.make_dataset(dataset)
            for dataset in datasets  # is making necessary
        ], remaining_records

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
                .where(ODC_DATASET.c.dataset_type_ref == product.id)
                .where(ODC_DATASET.c.updated > only_those_newer_than)
                .group_by("month")
                .order_by("month")
            )

    def outdated_years(self, product_id: int):
        updated_months = TIME_OVERVIEW.alias("updated_months")
        years = TIME_OVERVIEW.alias("years_needing_update")

        with self.index._active_connection() as conn:
            conn.execute(
                # Select years
                select(years.c.start_day)
                .where(years.c.period_type == "year")
                .where(
                    years.c.product_ref == product_id,
                )
                # Where there exist months that are more newly created.
                .where(
                    exists(
                        select(updated_months.c.start_day)
                        .where(updated_months.c.period_type == "month")
                        .where(
                            func.extract("year", updated_months.c.start_day)
                            == func.extract("year", years.c.start_day)
                        )
                        .where(
                            updated_months.c.product_ref == product_id,
                        )
                        .where(
                            updated_months.c.generation_time > years.c.generation_time
                        )
                    )
                )
            )

    def product_ds_count_per_period(self):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    PRODUCT.c.name,
                    TIME_OVERVIEW.c.start_day,
                    TIME_OVERVIEW.c.period_type,
                    TIME_OVERVIEW.c.dataset_count,
                )
                .select_from(TIME_OVERVIEW.join(PRODUCT))
                .where(TIME_OVERVIEW.c.product_ref == PRODUCT.c.id)
                .order_by(
                    PRODUCT.c.name,
                    TIME_OVERVIEW.c.start_day,
                    TIME_OVERVIEW.c.period_type,
                )
            )

    def upsert_product_record(self, product_name: str, **fields):
        # Dear future reader. This section used to use an 'UPSERT' statement (as in,
        # insert, on_conflict...) and while this works, it triggers the sequence
        # `product_id_seq` to increment as part of the check for insertion. This
        # is bad because there's only 32 k values in the sequence and we have run out
        # a couple of times! So, It appears that this update-else-insert must be done
        # in two transactions...
        with self.index._active_connection() as conn:
            row = conn.execute(
                select(PRODUCT.c.id, PRODUCT.c.last_refresh).where(
                    PRODUCT.c.name == product_name
                )
            ).fetchone()

            if row:
                # Product already exists, so update it
                return conn.execute(
                    PRODUCT.update()
                    .returning(PRODUCT.c.id, PRODUCT.c.last_refresh)
                    .where(PRODUCT.c.id == row[0])
                    .values(fields)
                ).fetchone()
            else:
                # Product doesn't exist, so insert it
                return conn.execute(
                    insert(PRODUCT)
                    .returning(PRODUCT.c.id, PRODUCT.c.last_refresh)
                    .values(**fields, name=product_name)
                ).fetchone()

    def put_summary(self, product_id: int, start_day, period, summary_row: dict):
        with self.index._active_connection() as conn:
            return conn.execute(
                insert(TIME_OVERVIEW)
                .returning(TIME_OVERVIEW.c.generation_time)
                .on_conflict_do_update(
                    index_elements=["product_ref", "start_day", "period_type"],
                    set_=summary_row,
                    where=and_(
                        TIME_OVERVIEW.c.product_ref == product_id,
                        TIME_OVERVIEW.c.start_day == start_day,
                        TIME_OVERVIEW.c.period_type == period,
                    ),
                )
                .values(
                    product_ref=product_id,
                    start_day=start_day,
                    period_type=period,
                    **summary_row,
                )
            )

    def product_summary_cols(self, product_name: str):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
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
                ).where(PRODUCT.c.name == product_name)
            ).fetchone()

    def upsert_product_regions(self, product_id: int):
        # add new regions row and/or update existing regions based on dataset_spatial
        with self.index._active_connection() as conn:
            return conn.execute(
                text(f"""
            with srid_groups as (
                select cubedash.dataset_spatial.dataset_type_ref                         as dataset_type_ref,
                        cubedash.dataset_spatial.region_code                             as region_code,
                        ST_Transform(ST_Union(cubedash.dataset_spatial.footprint), 4326) as footprint,
                        count(*)                                                         as count
                from cubedash.dataset_spatial
                where cubedash.dataset_spatial.dataset_type_ref = {product_id}
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

                """)
            )

    def delete_product_emtpy_regions(self, product_id: int):
        with self.index._active_connection() as conn:
            # not_empty_regions = select(
            #     DATASET_SPATIAL.c.region_code
            # ).where(
            #     DATASET_SPATIAL.c.dataset_type_ref == product.id
            # ).group_by(
            #     DATASET_SPATIAL.c.region_code
            # )

            # result = conn.execute(
            #     REGION.delete()
            #     .where(
            #         and_(
            #             DATASET_SPATIAL.c.dataset_type_ref == product.id,
            #             DATASET_SPATIAL.c.id.not_in(not_empty_regions)
            #         )
            #     )
            # )
            return conn.execute(
                text(f"""
            delete from cubedash.region
            where dataset_type_ref = {product_id} and region_code not in (
                select cubedash.dataset_spatial.region_code
                from cubedash.dataset_spatial
                where cubedash.dataset_spatial.dataset_type_ref = {product_id}
                group by cubedash.dataset_spatial.region_code
            )
                """),
            )

    def product_time_overview(self, product_id: int):
        with self.index._active_connection() as conn:
            # earliest, latest, total_count = conn.execute(
            #     select(
            #         func.min(DATASET_SPATIAL.c.center_time),
            #         func.max(DATASET_SPATIAL.c.center_time),
            #         func.count(),
            #     ).where(DATASET_SPATIAL.c.dataset_type_ref == product.id)
            # ).fetchone()
            return conn.execute(
                select(
                    TIME_OVERVIEW.c.time_earliest,
                    TIME_OVERVIEW.c.time_latest,
                    TIME_OVERVIEW.c.dataset_count,
                ).where(TIME_OVERVIEW.c.product_ref == product_id)
            ).fetchone()

    def product_time_summary(self, product_id: int, start_day, period):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(TIME_OVERVIEW).where(
                    and_(
                        TIME_OVERVIEW.c.product_ref == product_id,
                        TIME_OVERVIEW.c.start_day == start_day,
                        TIME_OVERVIEW.c.period_type == period,
                    )
                )
            )

    def latest_arrivals(self, period_length: timedelta):
        with self.index._active_connection() as conn:
            latest_arrival_date: datetime = conn.execute(
                text("select max(added) from agdc.dataset;")
            ).scalar()
            if latest_arrival_date is None:
                raise EmptyDbError()

            datasets_since_date = (latest_arrival_date - period_length).date()

            # shouldn't this be getting from agdc.dataset combined with dataset_spatial?
            # no point returning datasets that have been added in the odc database but not the cubedash one
            return conn.execute(
                text("""
                    select
                    date_trunc('day', added) as arrival_date,
                    (select name from agdc.dataset_type where id = d.dataset_type_ref) product_name,
                    count(*),
                    (array_agg(id))[0:3]
                    from agdc.dataset d
                    where d.added > :datasets_since
                    group by arrival_date, product_name
                    order by arrival_date desc, product_name;
                """),
                {
                    "datasets_since": datasets_since_date,
                },
            )

    def already_summarised_period(self, period: str, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(TIME_OVERVIEW.c.start_day).where(
                    and_(
                        TIME_OVERVIEW.c.product_ref == product_id,
                        TIME_OVERVIEW.c.period_type == period,
                    )
                )
            )

    def linked_products_search(self, product_id: int, sample_sql: str, direction: str):
        from_ref, to_ref = "source_dataset_ref", "dataset_ref"
        if direction == "derived":
            to_ref, from_ref = from_ref, to_ref

        # surely we could use existing linked datasets logic as part of this?
        with self.index._active_connection() as conn:
            return conn.execute(
                text(f"""
                with datasets as (
                    select id from agdc.dataset {sample_sql}
                    where dataset_type_ref={product_id}
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
            """)
            )

    def product_region_summary(self, product_id: int):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    REGION.c.region_code,
                    REGION.c.count,
                    REGION.c.generation_time,
                    REGION.c.footprint,
                )
                .where(REGION.c.dataset_type_ref == product_id)
                .order_by(REGION.c.region_code)
            )

    def dataset_footprint_region(self, dataset_id):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326).label(
                        "footprint"
                    ),
                    DATASET_SPATIAL.c.region_code,
                ).where(DATASET_SPATIAL.c.id == dataset_id)
            )

    def latest_dataset_added_time(self, product_id: int):
        # DATASET_SPATIAL doesn't keep track of when the dataset was indexed,
        # so we have to get that info from ODC_DATASET
        # join might not be necessary
        with self.index._active_connection() as conn:
            return conn.execute(
                select(func.max(ODC_DATASET.c.added))
                .select_from(
                    DATASET_SPATIAL.join(
                        ODC_DATASET, onclause=DATASET_SPATIAL.c.id == ODC_DATASET.c.id
                    )
                )
                .where(DATASET_SPATIAL.c.dataset_type_ref == product_id)
            ).scalar()

    def update_product_refresh_timestamp(
        self, product_id: int, refresh_timestamp: datetime
    ):
        with self.index._active_connection() as conn:
            conn.execute(
                PRODUCT.update()
                .where(PRODUCT.c.id == product_id)
                .where(
                    or_(
                        PRODUCT.c.last_successful_summary.is_(None),
                        PRODUCT.c.last_successful_summary
                        < refresh_timestamp.isoformat(),
                    )
                )
                .values(last_successful_summary=refresh_timestamp)
            )

    # does this add much value? and if so, is there a better way to do it?
    def find_fixed_columns(self, field_values, candidate_fields, sample_ids):
        # alt approach?
        # as_fields = self.index.datasets.make_select_fields(first_dataset_fields.keys())
        # filtered_fields = [field for field in as_fields if field.type_name in simple_field_types]
        # select(
        #     *[
        #         (
        #             func.every(
        #                 field.alchemy_expression == first_dataset_fields[field.name]
        #             )
        #         ).label(field.name)
        #         for field in filtered_fields
        #     ]
        # )
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    *[
                        (
                            func.every(
                                field.alchemy_expression == field_values[field_name]
                            )
                        ).label(field_name)
                        for field_name, field in candidate_fields
                    ]
                )
                .select_from(ODC_DATASET)
                .where(ODC_DATASET.c.id.in_([r for (r,) in sample_ids]))
            )

    # does this really add much value? and if so, is there a better way to do it?
    def all_products_location_samples(
        self, products: list[Product], sample_size: int = 50
    ):
        queries = []
        for product in products:
            subquery = (
                select(
                    literal(product.name).label("name"),
                    func.array_agg(
                        DATASET_LOCATION.c.uri_scheme
                        + ":"
                        + DATASET_LOCATION.c.uri_body
                    ).label("uris"),  # Postgis.DatasetLocation has a 'uri' field direct
                )
                .select_from(DATASET_LOCATION.join(ODC_DATASET))
                .where(ODC_DATASET.c.dataset_type_ref == product.id)
                .where(ODC_DATASET.c.archived.is_(None))
                .limit(sample_size)
            )
            queries.append(subquery)
        # can't we operate on the PRODUCT columns rather than doing it in a loop? along the lines of:
        # select(ODC_PRODUCT.c.name, array_agg(postgres._api._dataset_uri_field))
        # .select_from(ODC_DATASET_LOCATION.join(ODC_DATASET))
        # .where(ODC_DATASET.c.dataset_type_ref == ODC_PRODUCT.c.id)
        # .where(ODC_DATASET.c.archived.is_(None))
        # .limit(sample_size)
        # .group_by(ODC_PRODUCT.c.name)

        if queries:  # Don't run invalid SQL on empty database
            # surely there must be a better way to check the database isn't empty before we get to this point?
            with self.index._active_connection() as conn:
                return conn.execute(union_all(*queries))
        else:
            raise EmptyDbError()

    # This is tied to ODC's internal Dataset search implementation as there's no higher-level api to allow this.
    # When region_code is integrated into core (as is being discussed) this can be replaced.
    # pylint: disable=protected-access
    def datasets_by_region(
        self,
        product: Product,
        region_code: str,
        time_range: Range,
        limit: int,
        offset: int = 0,
    ) -> Generator[Dataset, None, None]:
        query = (
            select(*_DATASET_SELECT_FIELDS)
            .select_from(
                DATASET_SPATIAL.join(
                    ODC_DATASET, DATASET_SPATIAL.c.id == ODC_DATASET.c.id
                )
            )
            .where(
                DATASET_SPATIAL.c.region_code == bindparam("region_code", region_code)
            )
            .where(
                DATASET_SPATIAL.c.dataset_type_ref
                == bindparam("dataset_type_ref", product.id)
            )
        )
        if time_range:
            query = query.where(
                DATASET_SPATIAL.c.center_time > bindparam("from_time", time_range.begin)
            ).where(
                DATASET_SPATIAL.c.center_time < bindparam("to_time", time_range.end)
            )
        query = (
            query.order_by(DATASET_SPATIAL.c.center_time.desc())
            .limit(bindparam("limit", limit))
            .offset(bindparam("offset", offset))
        )
        with self.index._active_connection() as conn:
            return (
                self.index.datasets._make(res, full_info=True)
                for res in conn.execute(query).fetchall()
            )

    def products_by_region(
        self,
        region_code: str,
        time_range: Range,
        limit: int,
        offset: int = 0,
    ) -> Generator[int, None, None]:
        query = (
            select(DATASET_SPATIAL.c.dataset_type_ref)
            .distinct()
            .where(
                DATASET_SPATIAL.c.region_code == bindparam("region_code", region_code)
            )
        )
        if time_range:
            query = query.where(
                DATASET_SPATIAL.c.center_time > bindparam("from_time", time_range.begin)
            ).where(
                DATASET_SPATIAL.c.center_time < bindparam("to_time", time_range.end)
            )

        query = (
            query.order_by(DATASET_SPATIAL.c.dataset_type_ref)
            .limit(bindparam("limit", limit))
            .offset(bindparam("offset", offset))
        )
        with self.index._active_connection() as conn:
            return (res.dataset_type_ref for res in conn.execute(query).fetchall())

    def delete_datasets(
        self, product_id: int, after_date: datetime = None, full: bool = False
    ):
        with self.index._active_connection() as conn:
            # Forcing? Check every other dataset for removal, so we catch manually-deleted rows from the table.
            if full:
                return conn.execute(
                    DATASET_SPATIAL.delete()
                    .where(
                        DATASET_SPATIAL.c.dataset_type_ref == product_id,
                    )
                    .where(
                        ~DATASET_SPATIAL.c.id.in_(
                            select(ODC_DATASET.c.id).where(
                                ODC_DATASET.c.dataset_type_ref == product_id,
                            )
                        )
                    )
                ).rowcount

            # Remove any archived datasets from our spatial table.
            # we could replace this with a ds_search_returning but that would mean two executions instead of one
            archived_datasets = (
                select(ODC_DATASET.c.id)
                .where(ODC_DATASET.c.archived.isnot(None))
                .where(ODC_DATASET.c.dataset_type_ref == product_id)
            )
            if after_date is not None:
                archived_datasets = archived_datasets.where(
                    ODC_DATASET.c.updated > after_date
                )

            return conn.execute(
                DATASET_SPATIAL.delete().where(
                    DATASET_SPATIAL.c.id.in_(archived_datasets)
                )
            ).rowcount

            # more concise - but less readable
            # query = DATASET_SPATIAL.delete().where(DATASET_SPATIAL.c.dataset_type_ref == product_id)
            # odc_datasets = select(ODC_DATASET.c.id).where(ODC_DATASET.c.dataset_type_ref == product_id)
            # if not full:
            #     odc_datasets = odc_datasets.where(ODC_DATASET.c.archived.isnot(None))
            #     if after_date is not None:
            #         odc_datasets = odc_datasets.where(ODC_DATASET.c.update > after_date)
            #     query = query.where(DATASET_SPATIAL.c.id.in_(odc_datasets))
            # else:
            #     query = query.where(~DATASET_SPATIAL.c.id.in_(odc_datasets))
            # conn.execute(query).rowcount

            # potential alt approach?
            # active_datasets = (
            #     select(ODC_DATASET.c.id)
            #     .where(ODC_DATASET.c.dataset_type_ref == product_id)
            # )
            # if not full:
            #     active_datasets = active_datasets.where(ODC_DATASET.c.archived.is_(None))
            #     if after_date is not None:
            #         active_datasets = active_datasets.where(ODC_DATASET.c.updated > after_date)
            # conn.execute(
            #     DATASET_SPATIAL.delete()
            #     .where(
            #         and_(
            #             DATASET_SPATIAL.c.dataset_type_ref == product_id,
            #             ~DATASET_SPATIAL.c.id.in_(active_datasets)
            #         )
            #     )
            # ).rowcount

    def upsert_datasets(self, product_id, column_values, after_date):
        column_values["id"] = ODC_DATASET.c.id
        column_values["dataset_type_ref"] = ODC_DATASET.c.dataset_type_ref

        with self.index._active_connection() as conn:
            only_where = [
                ODC_DATASET.c.dataset_type_ref
                == bindparam(
                    "product_ref", product_id, type_=SmallInteger
                ),  # why the need for bindparam?
                ODC_DATASET.c.archived.is_(None),
            ]
            if after_date is not None:
                only_where.append(ODC_DATASET.c.updated > after_date)

            return conn.execute(
                insert(DATASET_SPATIAL)
                .values(**column_values)
                .where(and_(*only_where))
                .on_conflict_do_update(
                    # is this right?
                    index_elements=["id"],
                    set_=column_values,
                    where=DATASET_SPATIAL.c.id == ODC_DATASET.c.id,
                )
            ).rowcount

    def synthesize_dataset_footprint(self, rows, shapes):
        with self.index._active_connection() as conn:
            return conn.execute(
                DATASET_SPATIAL.update()
                .where(DATASET_SPATIAL.c.id == bindparam("dataset_id"))
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

    def dataset_spatial_field_exprs(self):
        geom = func.ST_Transform(DATASET_SPATIAL.c.footprint, 4326)
        field_exprs = dict(
            collection=(
                select(ODC_PRODUCT.c.name)
                .where(ODC_PRODUCT.c.id == DATASET_SPATIAL.c.dataset_type_ref)
                .scalar_subquery()
            ),
            datetime=DATASET_SPATIAL.c.center_time,
            creation_time=DATASET_SPATIAL.c.creation_time,
            geometry=geom,
            bbox=func.Box2D(geom).cast(String),
            region_code=DATASET_SPATIAL.c.region_code,
            id=DATASET_SPATIAL.c.id,
        )
        return field_exprs

    def spatial_select_query(self, *clauses, full: bool = False):
        query = select(clauses)
        if full:
            return query.select_from(
                DATASET_SPATIAL.join(
                    ODC_DATASET, onclause=ODC_DATASET.c.id == DATASET_SPATIAL.c.id
                )
            )
        return query.select_from(DATASET_SPATIAL)

    def select_spatial_stats(self):
        # the only reason this needs to be in the api is because of the dataset_type_ref column
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    SPATIAL_QUALITY_STATS.c.dataset_type_ref.label("product_ref"),
                    SPATIAL_QUALITY_STATS.c.count,
                    SPATIAL_QUALITY_STATS.c.missing_footprint,
                    SPATIAL_QUALITY_STATS.c.footprint_size,
                    SPATIAL_QUALITY_STATS.c.footprint_stddev,
                    SPATIAL_QUALITY_STATS.c.missing_srid,
                    SPATIAL_QUALITY_STATS.c.has_file_size,
                    SPATIAL_QUALITY_STATS.c.has_region,
                )
            )

    def schema_initialised(self) -> bool:
        """
        Do our DB schemas exist?
        """
        with self.index._active_connection() as conn:
            return _schema.has_schema(conn)

    def schema_compatible_info(self, for_writing_operations_too=False):
        """
        Schema compatibility information
        postgis version, if schema has latest changes (optional: and has updated column)
        """
        with self.index._active_connection() as conn:
            return _schema.get_postgis_versions(conn), _schema.is_compatible_schema(
                conn, ODC_DATASET.fullname, for_writing_operations_too
            )

    def init_schema(self, grouping_epsg_code: int):
        # with self.index._active_connection() as conn:
        with self.engine.begin() as conn:
            return _schema.init_elements(conn, grouping_epsg_code)

    def refresh_stats(self, concurrently=False):
        """
        Refresh general statistics tables that cover all products.

        This is ideally done once after all needed products have been refreshed.
        """
        with self.index._active_connection() as conn:
            _schema.refresh_supporting_views(conn, concurrently=concurrently)

    @lru_cache()
    def get_srid_name(self, srid: int):
        """
        Convert an internal postgres srid key to a string auth code: eg: 'EPSG:1234'
        """
        with self.index._active_connection() as conn:
            return _schema.get_srid_name(conn, srid)

    def summary_where_clause(
        self, product_name: str, begin_time: datetime, end_time: datetime
    ) -> ColumnElement:
        return and_(
            func.tstzrange(begin_time, end_time, "[]", type_=TSTZRANGE).contains(
                DATASET_SPATIAL.c.center_time
            ),
            DATASET_SPATIAL.c.dataset_type_ref
            == (
                select(ODC_PRODUCT.c.id).where(ODC_PRODUCT.c.name == product_name)
            ).scalar_subquery(),
            or_(
                func.st_isvalid(DATASET_SPATIAL.c.footprint).is_(True),
                func.st_isvalid(DATASET_SPATIAL.c.footprint).is_(None),
            ),
        )

    def srid_summary(self, where_clause: ColumnElement):
        select_by_srid = (
            select(
                func.ST_SRID(DATASET_SPATIAL.c.footprint).label("srid"),
                func.count().label("dataset_count"),
                func.ST_Transform(
                    func.ST_Union(DATASET_SPATIAL.c.footprint),
                    FOOTPRINT_SRID_EXPRESSION,
                    type_=Geometry(),
                ).label("footprint_geometry"),
                func.sum(DATASET_SPATIAL.c.size_bytes).label("size_bytes"),
                func.max(DATASET_SPATIAL.c.creation_time).label(
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

    def day_counts(self, grouping_time_zone, where_clause: ColumnElement):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    func.date_trunc(
                        "day",
                        DATASET_SPATIAL.c.center_time.op("AT TIME ZONE")(
                            grouping_time_zone
                        ),
                    ).label("day"),
                    func.count(),
                )
                .where(where_clause)
                .group_by("day")
            )

    def region_counts(self, where_clause):
        with self.index._active_connection() as conn:
            return conn.execute(
                select(
                    DATASET_SPATIAL.c.region_code.label("region_code"),
                    func.count(),
                )
                .where(where_clause)
                .group_by("region_code")
            )
