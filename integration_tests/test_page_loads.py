"""
Tests that load pages and check the contained text.
"""
import json
import re
from pathlib import Path
from typing import Dict, Tuple

import pytest
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient

import cubedash
from cubedash import _model
from cubedash.summary import SummaryStore
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from requests_html import HTML

TEST_DATA_DIR = Path(__file__).parent / 'data'

DEFAULT_TZ = tz.gettz('Australia/Darwin')


def _populate_from_dump(session_dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = session_dea_index.products.get_by_name(expected_type)
    dataset_count = 0

    create_dataset = Doc2Dataset(session_dea_index)

    for _, doc in read_documents(dump_path):
        label = doc['ga_label'] if ('ga_label' in doc) else doc['id']
        dataset, err = create_dataset(doc, f"file://example.com/test_dataset/{label}")
        assert dataset is not None, err
        created = session_dea_index.datasets.add(dataset)

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(scope='module', autouse=True)
def populate_index(module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        module_dea_index,
        'wofs_albers',
        TEST_DATA_DIR / 'wofs-albers-sample.yaml.gz'
    )
    _populate_from_dump(
        module_dea_index,
        'high_tide_comp_20p',
        TEST_DATA_DIR / 'high_tide_comp_20p.yaml.gz'
    )
    return module_dea_index


@pytest.fixture(scope='function')
def unpopulated_client(summary_store: SummaryStore) -> FlaskClient:
    _model.cache.clear()
    _model.STORE = summary_store
    _model.STORE.refresh_all_products()
    cubedash.app.config['TESTING'] = True
    return cubedash.app.test_client()


@pytest.fixture(scope='function')
def client(unpopulated_client: FlaskClient) -> FlaskClient:
    for product in _model.STORE.index.products.get_all():
        _model.STORE.get_or_update(product.name)
    return unpopulated_client


def test_default_redirect(client: FlaskClient):
    rv: Response = client.get('/', follow_redirects=False)
    # Redirect to a default.
    assert rv.location.endswith('/ls7_nbar_scene')


def test_get_overview(client: FlaskClient):
    html = _get_html(client, '/wofs_albers')
    check_dataset_count(html, 11)
    check_last_processed(html, '2018-05-20T11:25:35')
    assert 'Historic Flood Mapping Water Observations from Space' in html.text
    check_area('61,...km2', html)

    html = _get_html(client, '/wofs_albers/2017')

    check_dataset_count(html, 11)
    check_last_processed(html, '2018-05-20T11:25:35')
    assert 'Historic Flood Mapping Water Observations from Space' in html.text

    html = _get_html(client, '/wofs_albers/2017/04')
    check_dataset_count(html, 4)
    check_last_processed(html, '2018-05-20T09:36:57')
    assert 'Historic Flood Mapping Water Observations from Space' in html.text
    check_area('30,...km2', html)


def test_get_overview_product_links(client: FlaskClient):
    """
    Are the source and derived product lists being displayed?
    """
    html = _get_html(client, '/ls7_nbar_scene/2017')

    product_links = html.find('.source-product a')
    assert [l.text for l in product_links] == ['ls7_level1_scene']
    assert [l.attrs['href'] for l in product_links] == ['/ls7_level1_scene/2017']

    product_links = html.find('.derived-product a')
    assert [l.text for l in product_links] == ['ls7_pq_legacy_scene']
    assert [l.attrs['href'] for l in product_links] == ['/ls7_pq_legacy_scene/2017']


def test_get_day_overviews(client: FlaskClient):
    # Individual days are computed on-the-fly rather than from summaries, so can
    # have their own issues.

    # With a dataset
    html = _get_html(client, '/ls7_nbar_scene/2017/4/20')
    check_dataset_count(html, 1)

    # Empty day
    html = _get_html(client, '/ls7_nbar_scene/2017/4/22')
    check_dataset_count(html, 0)


def test_uninitialised_overview(unpopulated_client: FlaskClient, summary_store: SummaryStore):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    # Then load an unpopulated product.
    summary_store.get_or_update('ls7_nbar_albers')
    html = _get_html(unpopulated_client, '/ls7_nbar_scene/2017')
    assert html.find('.coverage-region-count', first=True).text == '0 unique scenes'


def test_view_dataset(client: FlaskClient):
    # ls7_level1_scene dataset
    rv: Response = client.get("/dataset/57848615-2421-4d25-bfef-73f57de0574d")
    # Label of dataset is header
    assert b'<h2>LS7_ETM_OTH_P51_GALPGS01-002_105_074_20170501</h2>' in rv.data

    # wofs_albers dataset (has no label or location)
    rv: Response = client.get("/dataset/20c024b5-6623-4b06-b00c-6b5789f81eeb")
    assert b'-20.502 to -19.6' in rv.data
    assert b'132.0 to 132.924' in rv.data


def test_view_product(client: FlaskClient):
    rv: Response = client.get("/product/ls7_nbar_scene")
    assert b'Landsat 7 NBAR 25 metre' in rv.data


def test_about_page(client: FlaskClient):
    rv: Response = client.get("/about")
    assert b"wofs_albers" in rv.data
    assert b'11 total datasets' in rv.data


@pytest.mark.skip(reason="TODO: fix out-of-date range return value")
def test_out_of_date_range(client: FlaskClient):
    """
    We have generated summaries for this product, but the date is out of the product's date range.
    """
    html = _get_html(client, '/wofs_albers/2010')

    # The common error here is to say "No data: not yet generated" rather than "0 datasets"
    assert check_dataset_count(html, 0)
    assert b'Historic Flood Mapping Water Observations from Space' in html.text


def test_loading_high_low_tide_comp(client: FlaskClient):
    html = _get_html(client, "/high_tide_comp_20p/2008")

    assert html.search('High Tide 20 percentage composites for entire coastline') is not None

    check_dataset_count(html, 306)
    # Footprint is not exact due to shapely.simplify()
    check_area('2,984,...km2', html)

    assert html.find('.last-processed time', first=True).attrs['datetime'] == '2017-06-08T20:58:07.014314+00:00'


def check_area(area_pattern, html):
    assert re.match(area_pattern + ' \(approx', html.find('.coverage-footprint-area', first=True).text)


def check_last_processed(html, time):
    __tracebackhide__ = True
    assert html.find('.last-processed time', first=True).attrs['datetime'].startswith(time)


def check_dataset_count(html, count: int):
    __tracebackhide__ = True
    assert f'{count} dataset' in html.find('.dataset-count', first=True).text


def test_api_returns_high_tide_comp_datasets(client: FlaskClient):
    """
    These are slightly fun to handle as they are a small number with a huge time range.
    """
    geojson = _get_geojson(client, '/api/datasets/high_tide_comp_20p')
    assert len(geojson['features']) == 306, "Not all high tide datasets returned as geojson"

    # Check that they're not just using the center time.
    # Within the time range, but not the center_time.
    # Range: '2000-01-01T00:00:00' to '2016-10-31T00:00:00'
    # year
    geojson = _get_geojson(client, '/api/datasets/high_tide_comp_20p/2000')
    assert len(geojson['features']) == 306, "Expected high tide datasets within whole dataset range"
    # month
    geojson = _get_geojson(client, '/api/datasets/high_tide_comp_20p/2009/5')
    assert len(geojson['features']) == 306, "Expected high tide datasets within whole dataset range"
    # day
    geojson = _get_geojson(client, '/api/datasets/high_tide_comp_20p/2016/10/1')
    assert len(geojson['features']) == 306, "Expected high tide datasets within whole dataset range"

    # Completely out of the test dataset time range. No results.
    geojson = _get_geojson(client, '/api/datasets/high_tide_comp_20p/2018')
    assert len(geojson['features']) == 0, "Expected no high tide datasets in in this year"


def test_api_returns_scenes_as_geojson(client: FlaskClient):
    """
    L1 scenes have no footprint, falls back to bounds. Have weird CRSes too.
    """
    geojson = _get_geojson(client, '/api/datasets/ls8_level1_scene')
    assert len(geojson['features']) == 7, "Unexpected scene polygon count"


def test_api_returns_tiles_as_geojson(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = _get_geojson(client, '/api/datasets/ls7_nbart_albers')
    assert len(geojson['features']) == 4, "Unepected albers polygon count"


def test_api_returns_high_tide_comp_regions(client: FlaskClient):
    """
    High tide doesn't have anything we can use as regions.

    It should be empty (no regions supported) rather than throw an exception.
    """
    geojson = _get_geojson(client, '/api/regions/high_tide_comp_20p')
    assert geojson == None


def test_api_returns_scene_regions(client: FlaskClient):
    """
    L1 scenes have no footprint, falls back to bounds. Have weird CRSes too.
    """
    geojson = _get_geojson(client, '/api/regions/ls8_level1_scene')
    assert len(geojson['features']) == 7, "Unexpected scene region count"


def test_region_page(client: FlaskClient):
    """
    Load a list of scenes for a given region.
    """
    html = _get_html(client, '/region/ls7_nbar_scene/96_82')
    search_results = html.find('.search-result a')
    assert len(search_results) == 1
    result = search_results[0]
    assert result.text == 'LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502'

    # If "I'm feeling lucky", and only one result, redirect straight to it.
    response: Response = client.get('/region/ls7_nbar_scene/96_82?feelinglucky')
    assert response.status_code == 302
    assert response.location.endswith('/dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c')


def test_search_page(client: FlaskClient):
    html = _get_html(client, '/datasets/ls7_nbar_scene')
    search_results = html.find('.search-result a')
    assert len(search_results) == 4

    html = _get_html(client, '/datasets/ls7_nbar_scene/2017/05')
    search_results = html.find('.search-result a')
    assert len(search_results) == 3


def test_search_time_completion(client: FlaskClient):
    # They only specified a begin time, so the end time should be filled in with the product extent.
    html = _get_html(client, '/datasets/ls7_nbar_scene?time-begin=1999-05-28')
    assert html.find('#search-time-before', first=True).attrs['value'] == '1999-05-28'
    # One day after the product extent end (range is exclusive)
    assert html.find('#search-time-after', first=True).attrs['value'] == '2017-05-04'
    search_results = html.find('.search-result a')
    assert len(search_results) == 4


def test_api_returns_tiles_regions(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = _get_geojson(client, '/api/regions/ls7_nbart_albers')
    assert len(geojson['features']) == 4, "Unexpected albers region count"


def test_api_returns_limited_tile_regions(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = _get_geojson(client, '/api/regions/wofs_albers/2017/04')
    assert len(geojson['features']) == 4, "Unexpected wofs albers region month count"
    geojson = _get_geojson(client, '/api/regions/wofs_albers/2017/04/20')
    print(json.dumps(geojson, indent=4))
    assert len(geojson['features']) == 1, "Unexpected wofs albers region day count"
    geojson = _get_geojson(client, '/api/regions/wofs_albers/2017/04/6')
    assert geojson is None, "Unexpected wofs albers region count"


def _get_geojson(client: FlaskClient, url: str) -> Dict:
    rv: Response = client.get(url)
    assert rv.status_code == 200
    response_geojson = json.loads(rv.data)
    return response_geojson


def _get_html_response(client: FlaskClient, url: str) -> Tuple[HTML, Response]:
    response: Response = client.get(url)
    assert response.status_code == 200
    html = HTML(html=response.data.decode('utf-8'))
    return html, response


def _get_html(client: FlaskClient, url: str) -> HTML:
    html, _ = _get_html_response(client, url)
    return html


def test_undisplayable_product(client: FlaskClient):
    """
    Telemetry products have no footprint available at all.
    """
    html = _get_html(client, '/ls7_satellite_telemetry_data')
    check_dataset_count(html, 4)
    assert '36.6GiB' in html.find('.coverage-filesize', first=True).text
    assert '(None displayable)' in html.text
    assert 'No CRSes defined' in html.text


def test_no_data_pages(client: FlaskClient):
    """
    Fetch products that exist but have no summaries generated.

    (these should load with "empty" messages: not throw exceptions)
    """
    html = _get_html(client, '/ls8_nbar_albers/2017')
    assert 'No data: not yet generated' in html.text
    assert 'Unknown number of datasets' in html.text

    html = _get_html(client, '/ls8_nbar_albers/2017/5')
    assert 'No data: not yet generated' in html.text
    assert 'Unknown number of datasets' in html.text

    # Days are generated on demand: it should query and see that there are no datasets.
    html = _get_html(client, '/ls8_nbar_albers/2017/5/2')
    check_dataset_count(html, 0)


def test_missing_dataset(client: FlaskClient):
    rv: Response = client.get('/datasets/f22a33f4-42f2-4aa5-9b20-cee4ca4a875c')
    assert rv.status_code == 404


def test_invalid_product(client: FlaskClient):
    rv: Response = client.get('/fake_test_product/2017')
    assert rv.status_code == 404
