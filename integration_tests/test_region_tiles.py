"""
Tests that indexes DEA C3 Summary products region tiles
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient
from flask import Response

from integration_tests.asserts import check_dataset_count, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_wo_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ga_ls_wo_fq_nov_mar_3-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ga_ls_wo_fq_nov_mar_3"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 5
    return module_dea_index


def test_wo_summary_product(client: FlaskClient):
    html = get_html(client, "/ga_ls_wo_fq_nov_mar_3")

    check_dataset_count(html, 5)


def test_wo_region_dataset_count(client: FlaskClient):
    html = get_html(client, "/product/ga_ls_wo_fq_nov_mar_3/regions/x11y46")

    search_results = html.find(".search-result a")
    assert len(search_results) == 5


# Test where region_code is defined in metadata but all are the same

@pytest.fixture(scope="module", autouse=True)
def populate_landcover_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ga_ls_landcover_class_cyear_2-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ga_ls_landcover_class_cyear_2"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 3
            print(ae)
    assert dataset_count == 3
    return module_dea_index


def test_landcover_summary_product(client: FlaskClient):
    html = get_html(client, "/ga_ls_landcover_class_cyear_2")

    check_dataset_count(html, 3)


def test_landcover_region_dataset_count(client: FlaskClient):
    html = get_html(client, "/product/ga_ls_landcover_class_cyear_2/regions/au")

    search_results = html.find(".search-result a")
    assert len(search_results) == 3


@pytest.fixture(scope="module", autouse=True)
def populate_tmad_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ls5_nbart_tmad_annual-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ls5_nbart_tmad_annual"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 2
            print(ae)
    assert dataset_count == 2
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ls7_nbart_tmad_annual-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ls7_nbart_tmad_annual"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 3
            print(ae)
    assert dataset_count == 3
    return module_dea_index


def test_tmad_summary_product(client: FlaskClient):
    html = get_html(client, "/ls5_nbart_tmad_annual")

    check_dataset_count(html, 2)


def test_tmad_region_dataset_count(client: FlaskClient):
    html = get_html(client, "product/ls5_nbart_tmad_annual/regions/-14_-25")

    search_results = html.find(".search-result a")
    assert len(search_results) == 1

    html = get_html(client, "product/ls5_nbart_tmad_annual/regions/8_-36")

    search_results = html.find(".search-result a")
    assert len(search_results) == 1


def test_tmad_archived_dataset_region(client: FlaskClient, run_generate, module_dea_index):
    try:
        # now  index one tile that sole represents a region
        module_dea_index.datasets.archive(['867050c5-f854-434b-8b16-498243a5cf24'])

        # ... the next generation should catch it and update with one less dataset....
        run_generate("ls5_nbart_tmad_annual")

        rv: Response = client.get(
            "product/ls5_nbart_tmad_annual/regions/8_-36"
        )
        assert rv.status_code == 404

    finally:
        # Now let's restore the dataset!
        module_dea_index.datasets.restore(['867050c5-f854-434b-8b16-498243a5cf24'])


def test_region_switchable_product(client: FlaskClient):
    # Two products share the same region code
    html = get_html(client, "/product/ls5_nbart_tmad_annual/regions/8_-36")
    product_list = html.find("#product-headers ul.items li:not(.empty)")
    assert len(product_list) == 2

    # Only one product has the region code
    html = get_html(client, "/product/ls5_nbart_tmad_annual/regions/-14_-25")
    product_list = html.find("#product-headers ul.items li:not(.empty)")
    assert len(product_list) == 1
