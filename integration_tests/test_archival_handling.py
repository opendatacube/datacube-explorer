"""
Please note, in this test case, one of the dataset has datetime within
end_datetime and start_datetime range (actual live dataset sample)
while the other dataset has datetime outside of
end_datetime and start_datetime range (deliberate test setup sample)
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.model import MetadataType
from datacube.utils import read_documents
from flask.testing import FlaskClient

from integration_tests.asserts import check_dataset_count, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def ls7_in_index(odc_test_db):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    for _, metadata_doc in read_documents(
        TEST_DATA_DIR / "metadata/eo3_landsat_ard.odc-type.yaml"
    ):
        odc_test_db.index.metadata_types.add(MetadataType(metadata_doc))
    for _, ga_ls7_product_doc in read_documents(
        TEST_DATA_DIR / "products/ga_ls7e_ard_3.odc-product.yaml"
    ):
        odc_test_db.index.products.add_document(ga_ls7_product_doc)
    for _, ga_ls7_dataset_doc in read_documents(
        TEST_DATA_DIR / "ga_ls7e_ard_3-sample.yaml"
    ):
        create_dataset = Doc2Dataset(odc_test_db.index)
        try:
            dataset, err = create_dataset(
                ga_ls7_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = odc_test_db.index.datasets.add(dataset)
            assert created.type.name == "ga_ls7e_ard_3"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 1
            print(ae)
    assert dataset_count == 1
    return odc_test_db


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


def test_post_archival_dataset_count(ls7_in_index, run_generate, client):
    ls7_in_index.index.datasets.archive(["50014f19-5546-4853-be8d-0185a798c083"])
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
