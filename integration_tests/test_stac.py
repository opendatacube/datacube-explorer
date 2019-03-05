"""
Tests that hit the stac api
"""
import json
from pathlib import Path
from pprint import pprint
from typing import Dict, Optional

import pytest
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient

import cubedash._stac
from cubedash import _model
from datacube.utils import validate_document
from integration_tests.asserts import get_geojson, get_json

DEFAULT_TZ = tz.gettz("Australia/Darwin")

# Smaller values to ease testing.
OUR_DATASET_LIMIT = 20
OUR_PAGE_SIZE = 4

_SCHEMA_DIR = Path(__file__).parent / "schemas" / "stac"

# https://raw.githubusercontent.com/radiantearth/stac-spec/master/item-spec/json-schema/item.json
_ITEM_SCHEMA_PATH = _SCHEMA_DIR / "item.json"
_ITEM_SCHEMA = json.load(_ITEM_SCHEMA_PATH.open("r"))
# https://raw.githubusercontent.com/radiantearth/stac-spec/master/catalog-spec/json-schema/catalog.json
_CATALOG_SCHEMA_PATH = _SCHEMA_DIR / "catalog.json"
_CATALOG_SCHEMA = json.load(_CATALOG_SCHEMA_PATH.open("r"))


def get_items(client: FlaskClient, url: str) -> Dict:
    data = get_geojson(client, url)
    # TODO: validate schema
    return data


def get_item(client: FlaskClient, url: str) -> Dict:
    data = get_json(client, url)
    _validate_item(data)
    return data


@pytest.fixture(scope="function")
def stac_client(populated_index, client: FlaskClient):
    """
    Get a client with populated data and standard settings
    """
    cubedash._stac.DATASET_LIMIT = OUR_DATASET_LIMIT
    cubedash._stac.DEFAULT_PAGE_SIZE = OUR_PAGE_SIZE
    return client


def test_stac_search_all_pages(stac_client: FlaskClient):
    # Search all products
    limit = OUR_PAGE_SIZE // 2
    geojson = get_items(
        stac_client,
        (
            f"/stac/search?"
            f"&bbox=[114, -33, 153, -10]"
            f"&time=2017-04-16T01:12:16/2017-05-10T00:24:21"
            f"&limit={limit}"
        ),
    )
    assert len(geojson.get("features")) == limit
    dataset_count = limit

    # Keep loading "next" pages and we should hit the DATASET_LIMIT.
    next_page_url = _get_next_href(geojson)
    while next_page_url:
        geojson = get_items(stac_client, next_page_url)
        assert len(geojson.get("features")) == limit
        dataset_count += limit
        next_page_url = _get_next_href(geojson)
    assert dataset_count == OUR_DATASET_LIMIT


def test_stac_search_limits(stac_client: FlaskClient):
    # Limit with value greater than DATASET_LIMIT should be truncated.
    geojson = get_items(
        stac_client,
        (
            f"/stac/search?"
            f"&bbox=[114, -33, 153, -10]"
            f"&time=2017-04-16T01:12:16/2017-05-10T00:24:21"
            f"&limit={OUR_DATASET_LIMIT + 2}"
        ),
    )
    assert len(geojson.get("features")) == OUR_DATASET_LIMIT

    # Without limit, it should use the default page size
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[114, -33, 153, -10]"
            "&time=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    assert len(geojson.get("features")) == OUR_PAGE_SIZE


def test_stac_search_bounds(stac_client: FlaskClient):
    # Outside the box there should be no results
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[20,-5,25,10]"
            "&time=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    assert len(geojson.get("features")) == 0

    # Search a whole-day for a scene
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "product=ls7_nbar_scene"
            "&bbox=[114, -33, 153, -10]"
            "&time=2017-04-20"
        ),
    )
    assert len(geojson.get("features")) == 1

    # Search a whole-day on an empty day.
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "product=ls7_nbar_scene"
            "&bbox=[114, -33, 153, -10]"
            "&time=2017-04-22"
        ),
    )
    assert len(geojson.get("features")) == 0


def test_stac_search_by_post(stac_client: FlaskClient):
    # Test POST, product, and assets
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "wofs_albers",
                "bbox": [114, -33, 153, -10],
                "time": "2017-04-16T01:12:16/2017-05-10T00:24:21",
                "limit": (OUR_PAGE_SIZE),
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    assert len(doc.get("features")) == OUR_PAGE_SIZE
    assert "water" in doc["features"][0]["assets"]
    assert doc["features"][0]["assets"]["water"].get("href")

    # Test high_tide_comp_20p with POST and assets
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "high_tide_comp_20p",
                "bbox": [114, -40, 147, -32],
                "time": "2000-01-01T00:00:00/2016-10-31T00:00:00",
                "limit": 5,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    bands = ["blue", "green", "nir", "red", "swir1", "swir2"]
    first_item = doc["features"][0]
    for band in bands:
        assert band in first_item["assets"]

    # Validate stac item with jsonschema
    _validate_item(first_item)


def test_stac_collections(stac_client: FlaskClient):
    response = get_json(stac_client, "/stac")

    assert response.get("id"), "No id for stac endpoint"

    # TODO: Values of these will probably come from user configuration?
    assert "title" in response
    assert "description" in response

    # A child link to each "collection" (product)
    child_links = [l for l in response["links"] if l["rel"] == "child"]
    other_links = [l for l in response["links"] if l["rel"] != "child"]

    # a "self" link.
    assert len(other_links) == 1
    assert other_links[0]["rel"] == "self"

    found_products = set()
    for child_link in child_links:
        product_name = child_link["title"]
        href = child_link["href"]

        print(f"Loading collection page for {product_name}: {repr(href)}")
        collection_data = get_json(stac_client, href)
        assert collection_data["id"] == product_name
        # TODO: assert items, properties, etc.
        found_products.add(product_name)

    # We should have seen all products in the index
    expected_products = set(dt.name for dt in _model.STORE.all_dataset_types())
    assert found_products == expected_products


def test_stac_item(stac_client: FlaskClient):
    # Load one stac dataset from the test data.
    response = get_item(
        stac_client,
        "/collections/wofs_albers/items/87676cf2-ef18-47b5-ba30-53a99539428d",
    )
    # Our item document can still be improved. This is ensuring changes are deliberate.
    pprint(response)
    assert response == {
        "id": "87676cf2-ef18-47b5-ba30-53a99539428d",
        "type": "Feature",
        "bbox": [
            120.527_607_997_473,
            -30.850_045_540_800_6,
            121.510_624_611_368,
            -29.906_840_507_281_5,
        ],
        "properties": {
            "cubedash:region_code": "-11_-34",
            "datetime": "2017-04-19T11:45:56+10:00",
            "odc:creation-time": "2018-05-20T17:57:51.178223+10:00",
            "odc:product": "wofs_albers",
        },
        "geometry": {
            "coordinates": [
                [
                    [121.423_986_912_228, -30.850_045_540_800_6],
                    [120.527_607_997_473, -30.784_505_852_831_2],
                    [120.767_242_829_485, -29.906_840_507_281_5],
                    [121.510_624_611_368, -29.960_785_496_049_7],
                    [121.423_986_912_228, -30.850_045_540_800_6],
                ]
            ],
            "type": "Polygon",
        },
        "assets": {
            "odc:location": {
                "href": "file://example.com/test_dataset/87676cf2-ef18-47b5-ba30-53a99539428d"
            },
            # TODO: The measurement has a blank path, which in ODC means it is loaded from the base location.
            # This should probably be replaced with an "eo:bands" definition.
            "water": {
                "href": "file://example.com/test_dataset/87676cf2-ef18-47b5-ba30-53a99539428d"
            },
        },
        "links": [
            {
                "rel": "self",
                "href": "/collections/wofs_albers/items/87676cf2-ef18-47b5-ba30-53a99539428d",
            },
            {"rel": "parent", "href": "/collections/wofs_albers"},
        ],
    }


def _validate_item(item: Dict):
    validate_document(item, _ITEM_SCHEMA, schema_folder=_ITEM_SCHEMA_PATH.parent)


def _get_next_href(geojson: Dict) -> Optional[str]:
    hrefs = [link["href"] for link in geojson.get("links", []) if link["rel"] == "next"]
    if not hrefs:
        return None

    assert len(hrefs) == 1, "Multiple next links found: " + ",".join(hrefs)
    [href] = hrefs
    return href
