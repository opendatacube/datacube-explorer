"""
Tests that load pages and check the contained text.
"""
from datetime import datetime
from pathlib import Path

import pytest
from dateutil.tz import tzutc
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from datacube.model import Range
from integration_tests.asserts import expect_values, get_html

TEST_DATA_DIR = Path(__file__).parent / 'data'


@pytest.fixture(scope='module', autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    loaded = dataset_loader(
        's2a_ard_granule',
        TEST_DATA_DIR / 's2a_ard_granule.yaml.gz'
    )
    assert loaded == 8
    return module_dea_index


def test_s2_ard_summary(run_generate, summary_store: SummaryStore):
    run_generate('s2a_ard_granule')
    expect_values(
        summary_store.update('s2a_ard_granule'),
        dataset_count=8,
        footprint_count=8,
        time_range=Range(
            begin=datetime(2017, 9, 30, 14, 30, tzinfo=tzutc()),
            end=datetime(2017, 10, 31, 14, 30, tzinfo=tzutc())
        ),
        newest_creation_time=datetime(2018, 7, 26, 23, 49, 25, 684327, tzinfo=tzutc()),
        timeline_period='day',
        timeline_count=31,
        crses={'EPSG:32753'},
        size_bytes=0,
    )


def test_s2a_l1_summary(run_generate, summary_store: SummaryStore):
    run_generate('s2a_level1c_granule')
    expect_values(
        summary_store.update('s2a_level1c_granule'),
        dataset_count=8,
        footprint_count=8,
        time_range=Range(
            begin=datetime(2017, 9, 30, 14, 30, tzinfo=tzutc()),
            end=datetime(2017, 10, 31, 14, 30, tzinfo=tzutc())
        ),
        newest_creation_time=datetime(2017, 10, 23, 1, 13, 7, tzinfo=tzutc()),
        timeline_period='day',
        timeline_count=31,
        crses={'EPSG:32753'},
        size_bytes=3442177050,
    )


def test_product_audit(unpopulated_client: FlaskClient, run_generate):
    run_generate()
    client = unpopulated_client

    res = get_html(client, '/product-audit/?timings')
    # print(res.html)

    largest_footprint_size = res.find('.footprint-size .search-result')
    assert len(largest_footprint_size) == 10
    largest_product_footprint = largest_footprint_size[0].find('.product-name', first=True).text

    assert largest_product_footprint == 'wofs_summary'
    largest_val = largest_footprint_size[0].find('.size-value', first=True).text
    assert largest_val == '32.2KiB'

    assert len(res.find('.unavailable-metadata .search-result')) == 10

