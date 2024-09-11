# from datetime import date, datetime
# from uuid import UUID
# from typing import Iterable

# from datacube.index.postgis import Index
# from datacube.model import Dataset, Product
# from datacube.drivers.postgis._schema import (
#     Dataset as ODC_DATASET,
#     Product as ODC_PRODUCT,
# )
# from sqlalchemy import (
#     func,
#     select,
# )

# from cubedash.index.api import ExplorerAbstractIndex


# class ExplorerIndex(ExplorerAbstractIndex):
#     name = "pgis_index"

#     def __init__(self, index: Index):
#         self.index = index

#     def get_datasets_derived(
#         self, dataset_id: UUID, limit=None
#     ) -> tuple[list[Dataset], int]:
#         derived_ids = self.index.lineage.get_derived_tree(dataset_id, max_depth=1).child_datasets()
#         if limit:
#             remaining_records = len(derived_ids) - limit
#             derived_ids = derived_ids[:limit]
#         else:
#             remaining_records = 0
#         return self.index.datasets.bulk_get(derived_ids), remaining_records

#     def get_dataset_sources(
#         self, dataset_id: UUID, limit=None
#     ) -> tuple[list[Dataset], int]:
#         """
#         Get the direct source datasets of a dataset, but without loading the whole upper provenance tree.

#         A limit can also be specified.

#         Returns a source dict and how many more sources exist beyond the limit.
#         """
#         source_ids = self.index.lineage.get_source_tree(dataset_id, max_depth=1).child_datasets()
#         if limit:
#             remaining_records = len(source_ids) - limit
#             source_ids = source_ids[:limit]
#         else:
#             remaining_records = 0

#         return self.index.datasets.bulk_get(source_ids), remaining_records

#     def find_months_needing_update(
#         self,
#         product_name: str,
#         only_those_newer_than: datetime,
#     ) -> Iterable[tuple[date, int]]:
#         """
#         What months have had dataset changes since they were last generated?
#         """
#         product = self.index.products.get_by_name_unsafe(product_name)

#         # Find the most-recently updated datasets and group them by month.
#         with self.index._active_connection() as conn:
#             return sorted(
#                 (month.date(), count) # count isn't even used outside of log.debug
#                 for month, count in conn.execute(
#                     select(
#                         func.date_trunc(
#                             "month", datetime_expression(product.metadata_type)
#                         ).label("month"),
#                         func.count(),
#                     )
#                     .where(ODC_DATASET.product_ref == product.id)
#                     .where(ODC_DATASET.updated > only_those_newer_than)
#                     .group_by("month")
#                     .order_by("month")
#                 )
#             )

#     def summarised_years(self, product_id: int):
#         ...

#     def outdated_years(self, product_id: int):
#         ...

#     def product_ds_count_per_period(self):
#         ...

#     def upsert_product_record(self, product: Product, **fields):
#         ...

#     def put_summary(self, product_id: int, summary_row: dict):
#         ...

#     def product_summary_cols(self, product_name: str):
#         ...
