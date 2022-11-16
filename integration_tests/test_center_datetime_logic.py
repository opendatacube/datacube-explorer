"""
Please note, in this test case, one of the dataset has datetime within
end_datetime and start_datetime range (actual live dataset sample)
while the other dataset has datetime outside of
end_datetime and start_datetime range (deliberate test setup sample)
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from integration_tests.asserts import (
    check_dataset_count,
    check_datesets_page_datestring,
    get_html,
)

TEST_DATA_DIR = Path(__file__).parent / "data"


pytest.mark.xfail(True, reason="rainfall data removed")
@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(
        TEST_DATA_DIR / "rainfall_chirps_daily-sample.yaml"
    ):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "rainfall_chirps_daily"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 2
            print(ae)
    assert dataset_count == 2
    return module_dea_index


pytest.mark.xfail(True, reason="rainfall data removed")
def test_datestring_on_dataset_page(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(
        client,
        "/products/rainfall_chirps_daily/datasets/35cbccee-cb07-51cf-85d2-6d2948957544",
    )

    check_datesets_page_datestring(html, "31st May 2019")

    html = get_html(
        client,
        "/products/rainfall_chirps_daily/datasets/35cbccee-cb07-51cf-85d2-6d2948957545",
    )

    check_datesets_page_datestring(html, "15th May 2019")


pytest.mark.xfail(True, reason="rainfall data removed")
def test_datestring_on_datasets_search_page(client: FlaskClient):
    html = get_html(client, "/products/rainfall_chirps_daily/datasets")

    assert "Time UTC: 2019-05-15 00:00:00" in [
        a.find("td", first=True).attrs["title"] for a in html.find(".search-result")
    ], "datestring does not match expected center_time recorded in dataset_spatial table"


pytest.mark.xfail(True, reason="rainfall data removed")
def test_datestring_on_regions_page(client: FlaskClient):
    html = get_html(client, "/product/rainfall_chirps_daily/regions/x210y106")

    assert "2019-05-15 00:00:00" in [
        a.find("td", first=True).text.strip() for a in html.find(".search-result")
    ], "datestring does not match expected center_time recorded in dataset_spatial table"


pytest.mark.xfail(True, reason="rainfall data removed")
def test_summary_center_datetime(client: FlaskClient):
    html = get_html(client, "/rainfall_chirps_daily/2019/5")
    check_dataset_count(html, 2)

    html = get_html(client, "/rainfall_chirps_daily/2019/5/15")
    check_dataset_count(html, 1)

    html = get_html(client, "/rainfall_chirps_daily/2019/5/31")
    check_dataset_count(html, 1)
