"""
Please note, in this test case, one of the dataset has datetime within
end_datetime and start_datetime range (actual live dataset sample)
while the other dataset has datetime outside of
end_datetime and start_datetime range (deliberate test setup sample)
"""

import pytest
from flask.testing import FlaskClient

from integration_tests.asserts import (
    check_dataset_count,
    check_datesets_page_datestring,
    get_html,
)

METADATA_TYPES = ["metadata/eo3_metadata.yaml"]
PRODUCTS = ["products/rainfall_chirps_daily.odc-product.yaml"]
DATASETS = ["datasets/rainfall_chirps_daily-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


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


def test_datestring_on_datasets_search_page(client: FlaskClient):
    html = get_html(client, "/products/rainfall_chirps_daily/datasets")

    assert (
        "Time UTC: 2019-05-15 00:00:00"
        in [
            a.find("td", first=True).attrs["title"] for a in html.find(".search-result")
        ]
    ), "datestring does not match expected center_time recorded in dataset_spatial table"


def test_datestring_on_regions_page(client: FlaskClient):
    html = get_html(client, "/product/rainfall_chirps_daily/regions/x210y106")

    assert (
        "2019-05-15 00:00:00"
        in [a.find("td", first=True).text.strip() for a in html.find(".search-result")]
    ), "datestring does not match expected center_time recorded in dataset_spatial table"


def test_summary_center_datetime(client: FlaskClient):
    html = get_html(client, "/rainfall_chirps_daily/2019/5")
    check_dataset_count(html, 2)

    html = get_html(client, "/rainfall_chirps_daily/2019/5/15")
    check_dataset_count(html, 1)

    html = get_html(client, "/rainfall_chirps_daily/2019/5/31")
    check_dataset_count(html, 1)
