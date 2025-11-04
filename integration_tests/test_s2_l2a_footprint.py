"""
Tests that load pages and check the contained text.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from datacube.model import Range
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from integration_tests.asserts import check_dataset_count, expect_values, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"

METADATA_TYPES = ["metadata/eo3_metadata.yaml"]
PRODUCTS = ["products/esa_s2_l2a.product.yaml"]
DATASETS = ["datasets/s2_l2a-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_summary_product(client: FlaskClient) -> None:
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/s2_l2a")

    check_dataset_count(html, 4)


def test_product_dataset(client: FlaskClient) -> None:
    # Check if all datasets are available to be viewed
    html = get_html(client, "/datasets/s2_l2a")

    assert len(html.css(".search-result")) == 5


def test_s2_l2a_summary(run_generate, summary_store: SummaryStore) -> None:
    run_generate("s2_l2a")
    expect_values(
        summary_store.get("s2_l2a"),
        dataset_count=4,
        footprint_count=4,
        time_range=Range(
            begin=datetime(2016, 10, 31, 14, 30, tzinfo=timezone.utc),
            end=datetime(2019, 6, 30, 14, 30, tzinfo=timezone.utc),
        ),
        newest_creation_time=datetime(2019, 6, 20, 11, 57, 34, tzinfo=timezone.utc),
        timeline_period="day",
        timeline_count=91,
        crses={"EPSG:32632", "EPSG:32630", "EPSG:32627"},
        size_bytes=0,
    )


def test_product_audit(unpopulated_client: FlaskClient, run_generate) -> None:
    run_generate()
    client = unpopulated_client

    res = get_html(client, "/product-audit/")
    assert (
        res.css_first(".unavailable-metadata .search-result .product-name").text(
            strip=True
        )
        == "s2_l2a"
    )
    assert (
        res.css_first(
            ".unavailable-metadata .search-result .missing-footprint"
        ).attributes["title"]
        == "0 of 5 missing footprint"
    )


def test_get_overview_date_selector(client: FlaskClient) -> None:
    # [1] = year, [2] = month, [3] = day
    # check when no year, month, day has been selected
    html = get_html(client, "/s2_l2a")
    menu = html.css("#product-headers .header-option")
    assert len(menu[1].css(".option-menu ul li")) == 3

    # check only year has been selected
    html = get_html(client, "/s2_l2a/2016")
    menu = html.css("#product-headers .header-option")
    assert len(menu[1].css(".option-menu ul li")) == 3
    assert len(menu[2].css(".option-menu ul li")) == 3

    # check month has been selected
    html = get_html(client, "/s2_l2a/2016/11")
    menu = html.css("#product-headers .header-option")

    assert len(menu[1].css(".option-menu ul li")) == 3
    assert len(menu[2].css(".option-menu ul li")) == 3
    assert len(menu[3].css(".option-menu ul li")) == 3

    # checking when day is selected
    html = get_html(client, "/s2_l2a/2016/11/9")
    menu = html.css("#product-headers .header-option")

    assert len(menu[1].css(".option-menu ul li")) == 3
    assert len(menu[2].css(".option-menu ul li")) == 3
    assert len(menu[3].css(".option-menu ul li")) == 3


def test_refresh_product(
    empty_client: FlaskClient, summary_store: SummaryStore
) -> None:
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    summary_store.refresh("s2_l2a")

    # Then load a completely uninitialised product.
    html = get_html(empty_client, "/datasets/s2_l2a")
    search_results = html.css(".search-result a")
    assert len(search_results) == 5
