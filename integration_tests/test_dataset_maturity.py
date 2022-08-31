"""
Indexes 20 datasets for ga_ls8c_ard_3,
- 4 datasets have maturity level: interim
- 16 datasets have maturity level: final
"""
from pathlib import Path
from cubedash._utils import center_time_from_metadata, default_utc
import pytz

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from cubedash.summary import SummaryStore

from flask.testing import FlaskClient
import datetime

from integration_tests.asserts import check_dataset_count, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ga_ls8c_ard_3-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ga_ls8c_ard_3"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 20
            print(ae)
    assert dataset_count == 20
    return module_dea_index


def test_product_fixed_metadata_by_sample_percentage(summary_store: SummaryStore):
    # There are 4 interim and 16 final maturity level datasets
    # at 100% (all 20 datasets), the same dictionary will be returned
    # 100% of the time
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_percentage=100,
    )

    assert fixed_fields == {
        "platform": "landsat-8",
        "instrument": "OLI_TIRS",
        "product_family": "ard",
        "format": "GeoTIFF",
        "eo_gsd": 15.0
    }

    # There are 4 interim and 16 final maturity level datasets
    # at 50% (10 datasets), there is a fair chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_percentage=50,
    )

    assert len(fixed_fields) >= 5

    # There are 4 interim and 16 final maturity level datasets
    # at 20% (4 datasets), there is a large chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_percentage=20,
    )

    assert len(fixed_fields) >= 5

    # There are 4 interim and 16 final maturity level datasets
    # at 5% (1 datasets), there is a large chance, maturity level
    # will be in the dictionary
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls8c_ard_3"),
        sample_percentage=5,
    )

    assert len(fixed_fields) >= 5