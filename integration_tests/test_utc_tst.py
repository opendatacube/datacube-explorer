"""
Tests that load pages and check the contained text.
"""
from collections import Counter
from pathlib import Path

import pytest
import pytz
from flask.testing import FlaskClient

from cubedash._utils import center_time_from_metadata, default_utc
from integration_tests.asserts import check_dataset_count, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"

METADATA_TYPES = [
    "metadata/eo_metadata.yaml",
    "metadata/landsat_l1_scene.yaml",
    "metadata/eo3_landsat_l1.odc-type.yaml",
]
PRODUCTS = [
    "products/ls5_fc_albers.odc-product.yaml",
    "products/ls5_scenes.odc-product.yaml",
    "products/ls7_scenes.odc-product.yaml",
    "products/ls8_scenes.odc-product.yaml",
    "products/usgs_ls7e_level1_1.odc-product.yaml",
    "products/dsm1sv10.odc-product.yaml",
]
DATASETS = ["datasets/ls5_fc_albers-sample.yaml", "usgs_ls7e_level1_1-sample.yaml"]


@pytest.fixture(scope="module", autouse=True)
def _populate_index(auto_odc_db):
    assert auto_odc_db == Counter({"ls5_fc_albers": 5, "usgs_ls7e_level1_1": 5})


def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/ls5_fc_albers")

    check_dataset_count(html, 5)


def test_yearly_dataset_count(client: FlaskClient):
    html = get_html(client, "/ls5_fc_albers/2010")
    check_dataset_count(html, 2)

    html = get_html(client, "/ls5_fc_albers/2011")
    check_dataset_count(html, 3)


def test_dataset_search_page_localised_time(client: FlaskClient):
    html = get_html(client, "/products/ls5_fc_albers/datasets/2011")

    assert "2011-01-01 09:03:13" in [
        a.find("td", first=True).text.strip() for a in html.find(".search-result")
    ], "datestring does not match expected center_time recorded in dataset_spatial table"

    assert "Time UTC: 2010-12-31 23:33:13" in [
        a.find("td", first=True).attrs["title"] for a in html.find(".search-result")
    ], "datestring does not match expected center_time recorded in dataset_spatial table"

    html = get_html(client, "/products/ls5_fc_albers/datasets/2010")

    assert "2010-12-31 09:56:02" in [
        a.find("td", first=True).text.strip() for a in html.find(".search-result")
    ], "datestring does not match expected center_time recorded in dataset_spatial table"


# Unit tests
def test_dataset_day_link(summary_store):
    index = summary_store.index
    ds = index.datasets.get("5da416a9-faed-4600-880d-033d0b0f7b85")
    t = center_time_from_metadata(ds)
    t = default_utc(t).astimezone(pytz.timezone("Australia/Darwin"))
    assert t.year == 2011
    assert t.month == 1
    assert t.day == 1


def test_dataset_search_page_ls7e_time(client: FlaskClient):
    html = get_html(client, "/products/usgs_ls7e_level1_1/datasets/2020/6/1")
    search_results = html.find(".search-result a")
    assert len(search_results) == 2

    html = get_html(client, "/products/usgs_ls7e_level1_1/datasets/2020/6/2")
    search_results = html.find(".search-result a")
    assert len(search_results) == 3
