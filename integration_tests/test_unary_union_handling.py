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
from integration_tests.asserts import check_dataset_count
from datacube.utils import read_documents
from datacube.index.hl import Doc2Dataset
from integration_tests.asserts import expect_values, get_html


TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    loaded = dataset_loader('s2_l2a', TEST_DATA_DIR / "s2_l2a_2020_dec.yaml.gz")
    assert loaded == 19613

    return module_dea_index



def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/s2_l2a")

    check_dataset_count(html, 19613)


def test_refresh_product(empty_client: FlaskClient, summary_store: SummaryStore):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    summary_store.refresh_product(summary_store.index.products.get_by_name("s2_l2a"))
    summary_store.get_or_update("s2_l2a")

    # Then load a completely uninitialised product.
    html = get_html(empty_client, "/datasets/s2_l2a")
    search_results = html.find(".search-result a")
    assert len(search_results) == 19613