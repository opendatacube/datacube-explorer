"""
Please note, in this test case, one of the dataset has datetime within
end_datetime and start_datetime range (actual live dataset sample)
while the other dataset has datetime outside of
end_datetime and start_datetime range (deliberate test setup sample)
"""

import pytest
from flask.testing import FlaskClient

from integration_tests.asserts import check_dataset_count, get_html

METADATA_TYPES = ["metadata/eo3_landsat_ard.odc-type.yaml"]
PRODUCTS = ["products/ga_ls7e_ard_3.odc-product.yaml"]
DATASETS = ["datasets/ga_ls7e_ard_3-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_pre_archival_dataset_count(client: FlaskClient):
    html = get_html(client, "/products/ga_ls7e_ard_3")
    check_dataset_count(html, 1)

    html = get_html(client, "/audit/dataset-counts")

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-- td.numeric", first=True
    ).text
    assert dataset_count == "1"

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-1999- td.numeric", first=True
    ).text
    assert dataset_count == "1"

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-1999-7 td.numeric", first=True
    ).text
    assert dataset_count == "1"


def test_post_archival_dataset_count(odc_test_db, run_generate, client):
    odc_test_db.index.datasets.archive(["50014f19-5546-4853-be8d-0185a798c083"])
    run_generate("ga_ls7e_ard_3")

    html = get_html(client, "/products/ga_ls7e_ard_3")
    check_dataset_count(html, 0)

    html = get_html(client, "/audit/dataset-counts")

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-- td.numeric", first=True
    ).text
    assert dataset_count == "0"

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-1999- td.numeric", first=True
    ).text
    assert dataset_count == "0"

    dataset_count = html.find(
        "table.data-table tr#ga_ls7e_ard_3-1999-7 td.numeric", first=True
    ).text
    assert dataset_count == "0"
