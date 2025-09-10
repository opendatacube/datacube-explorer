"""
Tests that load pages and check the contained text.
"""

from datetime import datetime, timezone

import pytest
from datacube.model import Range
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from integration_tests.asserts import expect_values, get_html

METADATA_TYPES = ["metadata/qga_eo.yaml"]
PRODUCTS = ["products/ga_s2_ard.odc-product.yaml"]
DATASETS = ["datasets/s2a_ard_granule.yaml.gz"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_s2_ard_summary(run_generate, summary_store: SummaryStore) -> None:
    run_generate("s2a_ard_granule")
    expect_values(
        summary_store.get("s2a_ard_granule"),
        dataset_count=8,
        footprint_count=8,
        time_range=Range(
            begin=datetime(2017, 9, 30, 14, 30, tzinfo=timezone.utc),
            end=datetime(2017, 10, 31, 14, 30, tzinfo=timezone.utc),
        ),
        newest_creation_time=datetime(
            2018, 7, 26, 23, 49, 25, 684_327, tzinfo=timezone.utc
        ),
        timeline_period="day",
        timeline_count=31,
        crses={"EPSG:32753"},
        size_bytes=0,
    )


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_s2a_l1_summary(run_generate, summary_store: SummaryStore) -> None:
    run_generate("s2a_level1c_granule")
    expect_values(
        summary_store.get("s2a_level1c_granule"),
        dataset_count=8,
        footprint_count=8,
        time_range=Range(
            begin=datetime(2017, 9, 30, 14, 30, tzinfo=timezone.utc),
            end=datetime(2017, 10, 31, 14, 30, tzinfo=timezone.utc),
        ),
        newest_creation_time=datetime(2017, 10, 23, 1, 13, 7, tzinfo=timezone.utc),
        timeline_period="day",
        timeline_count=31,
        crses={"EPSG:32753"},
        size_bytes=3_442_177_050,
    )


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_product_audit(unpopulated_client: FlaskClient, run_generate) -> None:
    run_generate()
    client = unpopulated_client

    res = get_html(client, "/product-audit/?timings")
    largest_footprint_size = res.css(".footprint-size .search-result")
    assert len(largest_footprint_size) == 2

    largest_product_footprint = (
        largest_footprint_size[0].css_first(".product-name").text(strip=True)
    )
    largest_val = largest_footprint_size[0].css_first(".size-value").text(strip=True)
    # They're both the same :/
    assert largest_product_footprint in ("s2a_ard_granule", "s2a_level1c_granule")
    assert largest_val == "181.6B"

    assert len(res.css(".unavailable-metadata .search-result")) == 2

    res = client.get("/audit/day-query-times.txt")
    plain_timing_results = res.data.decode("utf-8")
    assert '"s2a_ard_granule"\t8\t' in plain_timing_results
