"""
Indexes 20 datasets for ga_ls8c_ard_3,
- 4 datasets have maturity level: interim
- 16 datasets have maturity level: final
"""

from pathlib import Path

import pytest

from cubedash.summary import SummaryStore

TEST_DATA_DIR = Path(__file__).parent / "data"

METADATA_TYPES = [
    "metadata/eo3_metadata.yaml",
    "metadata/eo3_landsat_ard.odc-type.yaml",
]
PRODUCTS = ["products/ga_ls8c_ard_3.odc-product.yaml"]
DATASETS = ["datasets/ga_ls8c_ard_3-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_product_fixed_metadata_by_sample_percentage(
    summary_store: SummaryStore, client
):
    # There are 4 interim and 16 final maturity level datasets
    # at 100% (all 20 datasets), the same dictionary will be returned
    # 100% of the time
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_datasets_size=20,
    )

    assert fixed_fields == {
        "platform": "landsat-8",
        "instrument": "OLI_TIRS",
        "product_family": "ard",
        "format": "GeoTIFF",
        "eo_gsd": 15.0,
    }

    # There are 4 interim and 16 final maturity level datasets
    # at 50% (10 datasets), there is a fair chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_datasets_size=10,
    )

    assert len(fixed_fields) >= 5

    # There are 4 interim and 16 final maturity level datasets
    # at 20% (4 datasets), there is a large chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_datasets_size=4,
    )

    assert len(fixed_fields) >= 5

    # There are 4 interim and 16 final maturity level datasets
    # at 5% (1 datasets), there is a large chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_datasets_size=1,
    )

    assert len(fixed_fields) >= 5
