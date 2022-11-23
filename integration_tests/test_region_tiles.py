"""
Tests that indexes DEA C3 Summary products region tiles
"""
from collections import Counter

import pytest
from flask import Response
from flask.testing import FlaskClient

from integration_tests.asserts import check_dataset_count, get_html

METADATA_TYPES = ["metadata/eo3_metadata.yaml"]
PRODUCTS = [
    "products/ga_ls_wo_fq_nov_mar_3.odc-product.yaml",
    "products/ls5_nbart_tmad_annual.odc-product.yaml",
    "products/ls7_nbart_tmad_annual.odc-product.yaml",
    "products/ga_ls_landcover_class_cyear_2.odc-product.yaml",
]
DATASETS = [
    "ga_ls_wo_fq_nov_mar_3-sample.yaml",
    "ls5_nbart_tmad_annual-sample.yaml",
    "ls7_nbart_tmad_annual-sample.yaml",
    "ga_ls_landcover_class_cyear_2-sample.yaml",
]


@pytest.fixture(scope="module", autouse=True)
def _populate_index(auto_odc_db):
    assert auto_odc_db == Counter(
        {
            "ga_ls_wo_fq_nov_mar_3": 5,
            "ls5_nbart_tmad_annual": 2,
            "ls7_nbart_tmad_annual": 1,
            "ga_ls_landcover_class_cyear_2": 3,
        }
    )


def test_wo_summary_product(client: FlaskClient):
    html = get_html(client, "/ga_ls_wo_fq_nov_mar_3")

    check_dataset_count(html, 5)


def test_wo_region_dataset_count(client: FlaskClient):
    html = get_html(client, "/product/ga_ls_wo_fq_nov_mar_3/regions/x11y46")

    search_results = html.find(".search-result a")
    assert len(search_results) == 5


# Test where region_code is defined in metadata but all are the same


def test_landcover_summary_product(client: FlaskClient):
    html = get_html(client, "/ga_ls_landcover_class_cyear_2")

    check_dataset_count(html, 3)


def test_landcover_region_dataset_count(client: FlaskClient):
    html = get_html(client, "/product/ga_ls_landcover_class_cyear_2/regions/au")

    search_results = html.find(".search-result a")
    assert len(search_results) == 3


def test_tmad_summary_product(client: FlaskClient):
    html = get_html(client, "/ls5_nbart_tmad_annual")

    check_dataset_count(html, 2)


def test_tmad_archived_dataset_region(client: FlaskClient, run_generate, odc_test_db):
    html = get_html(client, "product/ls5_nbart_tmad_annual/regions/-14_-25")

    search_results = html.find(".search-result a")
    assert len(search_results) == 1

    html = get_html(client, "product/ls5_nbart_tmad_annual/regions/8_-36")

    search_results = html.find(".search-result a")
    assert len(search_results) == 1
    try:
        # now  index one tile that sole represents a region
        odc_test_db.index.datasets.archive(["867050c5-f854-434b-8b16-498243a5cf24"])

        # ... the next generation should catch it and update with one less dataset....
        run_generate("ls5_nbart_tmad_annual")

        rv: Response = client.get("product/ls5_nbart_tmad_annual/regions/8_-36")
        assert rv.status_code == 404

    finally:
        # Now let's restore the dataset!
        odc_test_db.index.datasets.restore(["867050c5-f854-434b-8b16-498243a5cf24"])


def test_region_switchable_product(client: FlaskClient):
    # Two products share the same region code
    html = get_html(client, "/product/ls5_nbart_tmad_annual/regions/8_-36")
    product_list = html.find("#product-headers ul.items li:not(.empty)")
    assert len(product_list) == 2

    # Only one product has the region code
    html = get_html(client, "/product/ls5_nbart_tmad_annual/regions/-14_-25")
    product_list = html.find("#product-headers ul.items li:not(.empty)")
    assert len(product_list) == 1
