"""
Tests that load pages and check the contained text.
"""
from datetime import datetime
from pathlib import Path

import pytest
from dateutil.tz import tzutc
from flask import Response
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
    product_ = module_dea_index.products.from_doc(s2_product_doc)
    module_dea_index.products.add(product_)
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "s2_l2a-sample.yaml"):
        print(s2_dataset_doc)
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "s2_l2a"
        except AttributeError as ae:
            print(ae)
    return module_dea_index


def test_s2_l2a_summary(run_generate, summary_store: SummaryStore):
    run_generate("s2_l2a")
    expect_values(
        summary_store.update("s2_l2a"),
        dataset_count=1,
        footprint_count=1,
        time_range=Range(
            begin=datetime(2017, 9, 30, 14, 30, tzinfo=tzutc()),
            end=datetime(2017, 10, 31, 14, 30, tzinfo=tzutc()),
        ),
        newest_creation_time=datetime(2018, 7, 26, 23, 49, 25, 684_327, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=31,
        crses={"EPSG:32753"},
        size_bytes=0,
    )


def test_product_audit(unpopulated_client: FlaskClient, run_generate):
    run_generate()
    client = unpopulated_client

    res = get_html(client, "/product-audit/?timings")
    # print(res.html)

    largest_footprint_size = res.find(".footprint-size .search-result")
    assert len(largest_footprint_size) == 2

    largest_product_footprint = (
        largest_footprint_size[0].find(".product-name", first=True).text
    )
    largest_val = largest_footprint_size[0].find(".size-value", first=True).text
    # They're both the same :/
    assert largest_product_footprint in ("s2a_ard_granule", "s2a_level1c_granule")
    assert largest_val == "181.6B"

    assert len(res.find(".unavailable-metadata .search-result")) == 2

    res: Response = client.get("/product-audit/day-times.txt")
    plain_timing_results = res.data.decode("utf-8")
    print(plain_timing_results)
    assert '"s2a_ard_granule"\t8\t' in plain_timing_results
