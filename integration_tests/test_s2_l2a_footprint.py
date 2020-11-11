"""
Tests that load pages and check the contained text.
"""
from datetime import datetime
from pathlib import Path

import pytest
from dateutil.tz import tzutc
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from datacube.model import Range
from datacube.utils import read_documents
from datacube.index.hl import Doc2Dataset
from integration_tests.asserts import expect_values, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    path, s2_product_doc = list(
        read_documents(TEST_DATA_DIR / "esa_s2_l2a.product.yaml")
    )[0]
    dataset_count = 0
    product_ = module_dea_index.products.from_doc(s2_product_doc)
    module_dea_index.products.add(product_)
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "s2_l2a-sample.yaml"):
        dataset, err = create_dataset(
            s2_dataset_doc, "file://example.com/test_dataset/"
        )
        assert dataset is not None, err
        created = module_dea_index.datasets.add(dataset)
        assert created.type.name == "s2_l2a"
        dataset_count += 1
    assert dataset_count == 2
    return module_dea_index


def test_s2_l2a_summary(run_generate, summary_store: SummaryStore):
    run_generate("s2_l2a")
    expect_values(
        summary_store.update("s2_l2a"),
        dataset_count=1,
        footprint_count=1,
        time_range=Range(
            begin=datetime(2019, 5, 31, 14, 30, tzinfo=tzutc()),
            end=datetime(2019, 6, 30, 14, 30, tzinfo=tzutc()),
        ),
        newest_creation_time=datetime(2019, 6, 20, 11, 57, 34, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=30,
        crses={"EPSG:32627"},
        size_bytes=0,
    )


def test_product_audit(unpopulated_client: FlaskClient, run_generate):
    run_generate()
    client = unpopulated_client

    res = get_html(client, "/product-audit/")
    print(res.html)

    assert (
        res.find(".unavailable-metadata .search-result .product-name", first=True).text
        == "s2_l2a"
    )
    assert (
        res.find(
            ".unavailable-metadata .search-result .missing-footprint", first=True
        ).attrs["title"]
        == "0 of 1 missing footprint"
    )
