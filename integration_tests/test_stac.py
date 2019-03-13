"""
Tests that hit the stac api
"""
import json
import pytest
from boltons.iterutils import research
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient
from pathlib import Path
from pprint import pprint, pformat
from shapely.geometry import shape as shapely_shape
from shapely.validation import explain_validity
from typing import Dict, Optional, Generator, Iterable

import cubedash._stac
from cubedash import _model
from datacube.utils import validate_document
from integration_tests.asserts import get_geojson, get_json, debug_help

DEFAULT_TZ = tz.gettz('Australia/Darwin')

# Smaller values to ease testing.
OUR_DATASET_LIMIT = 20
OUR_PAGE_SIZE = 4

_SCHEMA_DIR = Path(__file__).parent / 'schemas' / 'stac'

# https://raw.githubusercontent.com/radiantearth/stac-spec/master/item-spec/json-schema/item.json
_ITEM_SCHEMA_PATH = _SCHEMA_DIR / 'item.json'
_ITEM_SCHEMA = json.load(_ITEM_SCHEMA_PATH.open('r'))
# https://raw.githubusercontent.com/radiantearth/stac-spec/master/catalog-spec/json-schema/catalog.json
_CATALOG_SCHEMA_PATH = _SCHEMA_DIR / 'catalog.json'
_CATALOG_SCHEMA = json.load(_CATALOG_SCHEMA_PATH.open('r'))


def get_items(client: FlaskClient, url: str) -> Dict:
    with debug_help(f"Requested {repr(url)}"):
        data = get_geojson(client, url)
        assert_collection(data)
    return data


def get_item(client: FlaskClient, url: str) -> Dict:
    with debug_help(f"Requested {repr(url)}"):
        data = get_json(client, url)
        validate_item(data)
    return data


@pytest.fixture(scope='function')
def stac_client(populated_index, client: FlaskClient):
    """
    Get a client with populated data and standard settings
    """
    cubedash._stac.PAGE_SIZE_LIMIT = OUR_DATASET_LIMIT
    cubedash._stac.DEFAULT_PAGE_SIZE = OUR_PAGE_SIZE
    return client


def test_stac_loading_all_pages(stac_client: FlaskClient):
    # An unconstrained search returning every dataset.
    # It should return every dataset in order with no duplicates.
    all_items = _iter_items_across_pages(stac_client, f'/stac/search')
    validate_items(all_items, expect_count=393)

    # A constrained search within a bounding box.
    # It should return matching datasets in order with no duplicates.
    all_items = _iter_items_across_pages(
        stac_client,
        (
            f'/stac/search?'
            f'&bbox=[114, -33, 153, -10]'
            f'&time=2017-04-16T01:12:16/2017-05-10T00:24:21'
        )
    )
    validate_items(all_items, expect_count=66)


def validate_items(items: Iterable[Dict],
                   expect_ordered=True,
                   expect_count: int = None):
    """
    Check that a series of stac Items:
    - has no duplicates,
    - is ordered (center time: our default)
    - are all valid individually.
    - (optionally) has a specific count
    """
    seen_ids = set()
    last_item = None
    i = 0
    for item in items:
        id_ = item['id']
        with debug_help(f"Invalid item {i}, id {repr(str(id_))}", ):
            validate_item(item)

        # Assert there's no duplicates
        assert id_ not in seen_ids, (
            f"Duplicate dataset item (record {i}) of search results: {id_}"
        )
        seen_ids.add(id_)

        # Assert they are all ordered (including across pages!)
        if last_item and expect_ordered:
            # TODO: this is actually a (date, id) sort, but our test data has no duplicate dates.
            prev_dt = last_item['properties']['datetime']
            this_dt = item['properties']['datetime']
            assert prev_dt < this_dt, (
                f"Items {i} and {i - 1} out of order: {prev_dt} > {this_dt}"
            )
        i += 1

    # Note that the above block stops most paging infinite loops quickly
    # ("already seen this dataset id")
    # So we perform this length check in the same method and afterwards.
    if expect_count is not None:
        assert i == expect_count, f"Expected {expect_count} items"


def _iter_items_across_pages(
        client: FlaskClient,
        url: str
) -> Generator[Dict, None, None]:
    """
    Keep loading "next" pages and yield every stac Item in order
    """
    while url is not None:
        items = get_items(client, url)
        yield from items['features']
        url = _get_next_href(items)


def test_stac_search_limits(stac_client: FlaskClient):
    # Tell user with error if they request too much.
    large_limit = OUR_DATASET_LIMIT + 1
    rv: Response = stac_client.get(
        (
            f'/stac/search?'
            f'&limit={large_limit}'
        )
    )
    assert rv.status_code == 400
    assert b'Max page size' in rv.data

    # Without limit, it should use the default page size
    geojson = get_items(
        stac_client,
        (
            '/stac/search?'
            '&bbox=[114, -33, 153, -10]'
            '&time=2017-04-16T01:12:16/2017-05-10T00:24:21'
        )
    )
    assert len(geojson.get('features')) == OUR_PAGE_SIZE


def test_stac_search_bounds(stac_client: FlaskClient):
    # Outside the box there should be no results
    geojson = get_items(
        stac_client,
        (
            '/stac/search?'
            '&bbox=[20,-5,25,10]'
            '&time=2017-04-16T01:12:16/2017-05-10T00:24:21'
        )
    )
    assert len(geojson.get('features')) == 0

    # Search a whole-day for a scene
    geojson = get_items(
        stac_client,
        (
            '/stac/search?'
            'product=ls7_nbar_scene'
            '&bbox=[114, -33, 153, -10]'
            '&time=2017-04-20'
        )
    )
    assert len(geojson.get('features')) == 1

    # Search a whole-day on an empty day.
    geojson = get_items(
        stac_client,
        (
            '/stac/search?'
            'product=ls7_nbar_scene'
            '&bbox=[114, -33, 153, -10]'
            '&time=2017-04-22'
        )
    )
    assert len(geojson.get('features')) == 0


def test_stac_search_by_post(stac_client: FlaskClient):
    # Test POST, product, and assets
    rv: Response = stac_client.post(
        '/stac/search',
        data=json.dumps({
            'product': 'wofs_albers',
            'bbox': [114, -33, 153, -10],
            'time': '2017-04-16T01:12:16/2017-05-10T00:24:21',
            'limit': OUR_PAGE_SIZE,
        }),
        headers={'Content-Type': 'application/json',
                 'Accept': 'application/json'}
    )
    assert rv.status_code == 200
    doc = rv.json
    assert len(doc.get('features')) == OUR_PAGE_SIZE
    assert 'water' in doc['features'][0]['assets']
    assert doc['features'][0]['assets']['water'].get('href')

    # Test high_tide_comp_20p with POST and assets
    rv: Response = stac_client.post(
        '/stac/search',
        data=json.dumps({
            'product': 'high_tide_comp_20p',
            'bbox': [114, -40, 147, -32],
            'time': '2000-01-01T00:00:00/2016-10-31T00:00:00',
            'limit': 5,
        }),
        headers={'Content-Type': 'application/json',
                 'Accept': 'application/json'}
    )
    assert rv.status_code == 200
    doc = rv.json

    # Features should in include all bands.
    bands = ('blue', 'green', 'nir', 'red', 'swir1', 'swir2')
    for feature in doc['features']:
        with debug_help(f"feature {feature['id']}"):
            assert len(feature['assets']) == 1, "Expected only one"
            (name, asset), = feature['assets'].items()
            assert name == 'location'
            assert tuple(sorted(asset['eo:bands'])) == bands

            # Validate stac item with jsonschema
            validate_item(feature)


def test_stac_collections(stac_client: FlaskClient):
    response = get_json(stac_client, '/stac')

    assert response.get('id'), "No id for stac endpoint"

    # TODO: Values of these will probably come from user configuration?
    assert 'title' in response
    assert 'description' in response

    # A child link to each "collection" (product)
    child_links = [l for l in response['links'] if l['rel'] == 'child']
    other_links = [l for l in response['links'] if l['rel'] != 'child']

    # a "self" link.
    assert len(other_links) == 1
    assert other_links[0]['rel'] == 'self'

    found_products = set()
    for child_link in child_links:
        product_name = child_link['title']
        href = child_link['href']

        print(f"Loading collection page for {product_name}: {repr(href)}")
        collection_data = get_json(stac_client, href)
        assert collection_data['id'] == product_name
        # TODO: assert items, properties, etc.
        found_products.add(product_name)

    # We should have seen all products in the index
    expected_products = set(dt.name for dt in _model.STORE.all_dataset_types())
    assert found_products == expected_products


def test_stac_collection_items(stac_client: FlaskClient):
    """
    Follow the links to the "high_tide_comp_20p" collection and ensure it includes
    all of our tests data.
    """

    collections = get_json(stac_client, '/stac')
    for link in collections['links']:
        if link['rel'] == 'child' and link['title'] == 'high_tide_comp_20p':
            collection_href = link['href']
            break
    else:
        assert False, "high_tide_comp_20p not found in collection list"

    scene_collection = get_json(stac_client, collection_href)
    pprint(scene_collection)
    assert scene_collection == {
        'stac_version': '0.6.0',
        'id': 'high_tide_comp_20p',
        'title': 'high_tide_comp_20p',
        'properties': {},
        'description': 'High Tide 20 percentage composites for entire coastline',
        'extent': {'spatial': [112.22305899076751,
                               -43.8291965530654,
                               153.98505442492277,
                               -10.237104814250783],
                   'temporal': ['2008-06-01T11:00:00+10:00',
                                '2008-06-01T11:00:00+10:00']},

        'links': [
            {
                'href': 'http://localhost/collections/high_tide_comp_20p/items',
                'rel': 'items'
            }
        ],
        'providers': [],
    }

    item_links = scene_collection['links'][0]['href']
    validate_items(
        _iter_items_across_pages(stac_client, item_links),
        expect_count=306,
    )


def test_stac_item(stac_client: FlaskClient):
    # Load one stac dataset from the test data.
    response = get_item(
        stac_client,
        '/collections/wofs_albers/items/87676cf2-ef18-47b5-ba30-53a99539428d'
    )
    # Our item document can still be improved. This is ensuring changes are deliberate.
    pprint(response)
    assert response == {
        'id': '87676cf2-ef18-47b5-ba30-53a99539428d',
        'type': 'Feature',
        'bbox': [120.527607997473, -30.8500455408006,
                 121.510624611368, -29.9068405072815],
        'properties': {
            'datetime': '2017-04-19T11:45:56+10:00',
            'eo:platform': 'landsat-8',
            'eo:instrument': 'OLI_TIRS',
            'cubedash:region_code': '-11_-34',
            'odc:creation-time': '2018-05-20T17:57:51.178223+10:00',
            'odc:product': 'wofs_albers',
        },
        'geometry': {
            'type': 'Polygon',
            'coordinates': [[[121.42398691222829, -30.850045540800554],
                             [120.52760799747303, -30.784505852831213],
                             [120.76724282948523, -29.90684050728149],
                             [121.5106246113678, -29.96078549604967],
                             [121.42398691222829, -30.850045540800554]]],
        },
        'assets': {
            'water': {
                'href': 'file://example.com/test_dataset/87676cf2-ef18-47b5-ba30-53a99539428d',
                'odc:secondary_hrefs': [],
                # TODO: we're supposed to map bands to integers in stac.
                'eo:bands': ['water'],
            }
        },
        'links': [
            {
                'rel': 'self',
                'href': '/collections/wofs_albers/items/87676cf2-ef18-47b5-ba30-53a99539428d',
            },
            {
                'rel': 'parent',
                'href': '/collections/wofs_albers',
            },
            {
                'rel': 'alternative',
                'type': 'text/html',
                'href': '/dataset/87676cf2-ef18-47b5-ba30-53a99539428d',
            },
        ]
    }


def assert_collection(collection: Dict):
    assert 'features' in collection, "No features in collection"
    validate_items(collection['features'])


def validate_item(item: Dict):
    validate_document(
        item,
        _ITEM_SCHEMA,
        schema_folder=_ITEM_SCHEMA_PATH.parent,
    )

    # Should be a valid polygon
    assert 'geometry' in item, "Item has no geometry"
    assert item['geometry'], "Item has blank geometry"
    with debug_help(f"Failing shape:\n{pformat(item['geometry'])}"):
        shape = shapely_shape(item['geometry'])
        assert shape.is_valid, f"Item has invalid geometry: {explain_validity(shape)}"
        assert shape.geom_type in ('Polygon', 'MultiPolygon'), (
            "Unexpected type of shape"
        )

    # href should never be blank if present
    # -> The jsonschema enforces href as required, but it's not checking for emptiness.
    #    (and we've had empty ones in previous prototypes)
    for offset, value in research(item, lambda p, k, v: k == 'href'):
        viewable_offset = "→".join(map(repr, offset))
        assert value.strip(), f"href has empty value: {repr(viewable_offset)}"


def _get_next_href(geojson: Dict) -> Optional[str]:
    hrefs = [link['href'] for link in geojson.get('links', []) if link['rel'] == 'next']
    if not hrefs:
        return None

    assert len(hrefs) == 1, "Multiple next links found: " + ",".join(hrefs)
    [href] = hrefs
    return href

