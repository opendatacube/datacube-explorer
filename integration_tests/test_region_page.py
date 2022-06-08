"""
Tests that load pages and check the contained text.
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from integration_tests.asserts import (
    check_product_date_selector_not_contain,
    check_product_date_selector_contains,
    get_html,
)

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ls5_sr-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ls5_sr"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 6
            print(ae)
    assert dataset_count == 6
    return module_dea_index


def test_product_region_page_dataset_count(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/product/ls5_sr/regions/168053")

    search_results = html.find(".search-result")
    assert len(search_results) == 2

    html = get_html(client, "/product/ls5_sr/regions/205050")

    search_results = html.find(".search-result")
    assert len(search_results) == 4


def test_product_region_page_date_selector(client: FlaskClient):
    html = get_html(client, "/product/ls5_sr/regions/168053")
    check_product_date_selector_contains(
        html, "1984"
    )
    check_product_date_selector_not_contain(
        html, "1989"
    )
    check_product_date_selector_not_contain(
        html, "2007"
    )

    html = get_html(client, "/product/ls5_sr/regions/168053/1984")
    check_product_date_selector_contains(
        html, "1984", "October"
    )

    check_product_date_selector_not_contain(
        html, "1984", "June"
    )

    html = get_html(client, "/product/ls5_sr/regions/168053/1984/10")
    check_product_date_selector_contains(
        html, "1984", "October", "30th"
    )
