"""
Tests that load pages and check the contained text.
"""
from pathlib import Path

import pytest
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from integration_tests.asserts import check_dataset_count
from datacube.utils import read_documents
from datacube.index.hl import Doc2Dataset
from integration_tests.asserts import get_html


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
    for _, s2_dataset_doc in read_documents(
        TEST_DATA_DIR / "s2_l2a_unary_union_sample_nov.yaml"
    ):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "s2_l2a"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 3887

    return module_dea_index


def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/s2_l2a")

    check_dataset_count(html, 3887)


def test_refresh_product(empty_client: FlaskClient, summary_store: SummaryStore):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    summary_store.refresh_product(summary_store.index.products.get_by_name("s2_l2a"))
    summary_store.get_or_update("s2_l2a")

    # Then load a completely uninitialised product.
    html = get_html(empty_client, "/datasets/s2_l2a")
    search_results = html.find(".search-result a")
    assert len(search_results) == 150
