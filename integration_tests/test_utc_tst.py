"""
Tests that load pages and check the contained text.
"""
from pathlib import Path

import pytest
import pytz
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from cubedash._utils import center_time_from_metadata, default_utc
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
    for _, s2_dataset_doc in read_documents(
        TEST_DATA_DIR / "ls5_fc_albers-sample.yaml"
    ):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ls5_fc_albers"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 5
    return module_dea_index


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


@pytest.fixture(scope="module", autouse=True)
def populate_ls7e_level1_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(
        TEST_DATA_DIR / "usgs_ls7e_level1_1-sample.yaml"
    ):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "usgs_ls7e_level1_1"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 5
    return module_dea_index


def test_dataset_search_page_ls7e_time(client: FlaskClient):
    html = get_html(client, "/products/usgs_ls7e_level1_1/datasets/2020/6/1")
    search_results = html.find(".search-result a")
    assert len(search_results) == 2

    html = get_html(client, "/products/usgs_ls7e_level1_1/datasets/2020/6/2")
    search_results = html.find(".search-result a")
    assert len(search_results) == 3
