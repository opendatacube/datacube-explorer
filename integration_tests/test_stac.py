"""
Tests that hit the stac api
"""

import json
import urllib.parse
from collections import Counter
from collections import defaultdict
from pathlib import Path
from pprint import pformat, pprint
from typing import Dict, Generator, Iterable, Optional, Union

import jsonschema
import pytest
from boltons.iterutils import research
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient
from jsonschema import SchemaError
from shapely.geometry import shape as shapely_shape
from shapely.validation import explain_validity

import cubedash._stac
from cubedash import _model
from datacube.utils import read_documents
from .asserts import DebugContext, get_geojson, get_json

DEFAULT_TZ = tz.gettz("Australia/Darwin")

# Smaller values to ease testing.
OUR_DATASET_LIMIT = 20
OUR_PAGE_SIZE = 4

_SCHEMA_BASE = Path(__file__).parent / "schemas"
_STAC_SCHEMA_BASE = _SCHEMA_BASE / "stac"

_SCHEMAS_BY_NAME = defaultdict(list)
for schema_path in _SCHEMA_BASE.rglob("*.json"):
    _SCHEMAS_BY_NAME[schema_path.name].append(schema_path)


def read_document(path: Path) -> dict:
    """
    Read and parse exactly one document.
    """
    ds = list(read_documents(path))
    if len(ds) != 1:
        raise ValueError(f"Expected only one document to be in path {path}")

    _, doc = ds[0]
    return doc


def load_validator(schema_location: Path) -> jsonschema.Draft4Validator:

    # Allow schemas to reference other schemas in the same folder.
    def local_reference(ref):
        relative_path = schema_location.parent.joinpath(ref)
        if relative_path.exists():
            return read_document(relative_path)

        # This is a sloppy workaround.
        # Python jsonschema strips all parent-folder references ("../../"), so none of the relative
        # paths in stac work. We fallback to matching based on filename.
        similar_schemas = _SCHEMAS_BY_NAME.get(Path(ref).name)
        if similar_schemas:
            if len(similar_schemas) > 1:
                raise NotImplementedError(
                    f"cannot distinguish schema {ref!r} (within {schema_location}"
                )
            [presumed_schema] = similar_schemas
            return read_document(presumed_schema)
        raise ValueError(
            f"Schema reference not found: {ref!r} (within {schema_location})"
        )

    def web_reference(ref: str):
        """
        A reference to a schema via a URL

        eg http://geojson.org/schemas/Features.json'
        """
        (scheme, netloc, offset, params, query, fragment) = urllib.parse.urlparse(ref)
        # We used `wget -r` to download the remote schemas locally.
        # It puts into hostname/path folders by default. Eg. 'geojson.org/schema/Feature.json'
        path = _SCHEMA_BASE / f"{netloc}{offset}"
        if not path.exists():
            raise ValueError(
                f"No local copy exists of schema {ref!r}.\n"
                f"\tPerhaps we need to add it to ./update.sh in the tests folder?\n"
                f"\t(looked in {path})"
            )
        return read_document(path)

    if not schema_location.exists():
        raise ValueError(f"No jsonschema file found at {schema_location}")

    with schema_location.open("r") as s:
        try:
            schema = json.load(s)
        except json.JSONDecodeError as e:
            # Some in the repo have not been valid before...
            raise RuntimeError(
                f"Invalid json, cannot load schema {schema_location}"
            ) from e

    try:
        jsonschema.Draft7Validator.check_schema(schema)
    except SchemaError as e:
        raise RuntimeError(f"Invalid schema {schema_location}") from e

    ref_resolver = jsonschema.RefResolver.from_schema(
        schema,
        handlers={"": local_reference, "https": web_reference, "http": web_reference},
    )
    return jsonschema.Draft7Validator(schema, resolver=ref_resolver)


# Run `./update.sh` in the schema dir to check for newer versions of these.
_CATALOG_SCHEMA = load_validator(
    _STAC_SCHEMA_BASE / "catalog-spec/json-schema/catalog.json"
)
_COLLECTION_SCHEMA = load_validator(
    _STAC_SCHEMA_BASE / "collection-spec/json-schema/collection.json"
)
_ITEM_SCHEMA = load_validator(_STAC_SCHEMA_BASE / "item-spec/json-schema/item.json")
_ITEM_COLLECTION_SCHEMA = load_validator(
    _STAC_SCHEMA_BASE / "item-spec/json-schema/itemcollection.json"
)

_STAC_EXTENSIONS = dict(
    (extension.name, load_validator(extension / "json-schema" / "schema.json"))
    for extension_dir in _SCHEMA_BASE.rglob("extensions")
    for extension in extension_dir.iterdir()
)


def get_collection(client: FlaskClient, url: str, validate=True) -> Dict:
    """
    Get a URL, expecting a valid stac collection document to be there"""
    with DebugContext(f"Requested {repr(url)}"):
        data = get_json(client, url)
        if validate:
            assert_collection(data)
    return data


def get_items(client: FlaskClient, url: str) -> Dict:
    """
    Get a URL, expecting a valid stac item collection document to be there"""
    with DebugContext(f"Requested {repr(url)}"):
        data = get_geojson(client, url)
        assert_item_collection(data)
    return data


def get_item(client: FlaskClient, url: str) -> Dict:
    """
    Get a URL, expecting a single valid Stac Item to be there
    """
    with DebugContext(f"Requested {repr(url)}"):
        data = get_json(client, url)
        validate_item(data)
    return data


@pytest.fixture()
def stac_client(populated_index, client: FlaskClient):
    """
    Get a client with populated data and standard settings
    """
    cubedash._stac.PAGE_SIZE_LIMIT = OUR_DATASET_LIMIT
    cubedash._stac.DEFAULT_PAGE_SIZE = OUR_PAGE_SIZE
    _model.app.config["CUBEDASH_DEFAULT_LICENSE"] = "CC-BY-4.0"
    return client


def test_stac_loading_all_pages(stac_client: FlaskClient):
    # An unconstrained search returning every dataset.
    # It should return every dataset in order with no duplicates.
    all_items = _iter_items_across_pages(stac_client, "/stac/search")
    validate_items(
        all_items,
        expect_count=dict(
            pq_count_summary=20,
            dsm1sv10=1,
            high_tide_comp_20p=306,
            wofs_albers=11,
            ls8_nbar_scene=7,
            ls8_level1_scene=7,
            ls8_nbart_scene=7,
            ls8_pq_legacy_scene=7,
            ls8_nbart_albers=7,
            ls8_satellite_telemetry_data=7,
            ls7_nbart_albers=4,
            ls7_nbart_scene=4,
            ls7_nbar_scene=4,
            ls7_pq_legacy_scene=4,
            ls7_level1_scene=4,
        ),
    )

    # A constrained search within a bounding box.
    # It should return matching datasets in order with no duplicates.
    all_items = _iter_items_across_pages(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[114, -33, 153, -10]"
            "&time=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    validate_items(
        all_items,
        expect_count=dict(
            wofs_albers=11,
            ls8_nbar_scene=7,
            ls8_level1_scene=7,
            ls8_nbart_scene=7,
            ls8_pq_legacy_scene=7,
            ls8_nbart_albers=7,
            ls8_satellite_telemetry_data=6,
            ls7_nbart_albers=4,
            ls7_nbart_scene=4,
            ls7_nbar_scene=4,
            ls7_pq_legacy_scene=4,
            ls7_level1_scene=4,
        ),
    )


def validate_items(
    items: Iterable[Dict], expect_ordered=True, expect_count: Union[int, dict] = None
):
    """
    Check that a series of stac Items:
    - has no duplicates,
    - is ordered (center time: our default)
    - are all valid individually.
    - (optionally) has a specific count
    """
    __tracebackhide__ = True
    seen_ids = set()
    last_item = None
    i = 0
    product_counts = Counter()
    for item in items:
        id_ = item["id"]
        with DebugContext(f"Invalid item {i}, id {repr(str(id_))}"):
            validate_item(item)
        product_counts[item["properties"]["odc:product"]] += 1

        # Assert there's no duplicates
        assert (
            id_ not in seen_ids
        ), f"Duplicate dataset item (record {i}) of search results: {id_}"
        seen_ids.add(id_)

        # Assert they are all ordered (including across pages!)
        if last_item and expect_ordered:
            # TODO: this is actually a (date, id) sort, but our test data has no duplicate dates.
            prev_dt = last_item["properties"]["datetime"]
            this_dt = item["properties"]["datetime"]
            assert (
                prev_dt < this_dt
            ), f"Items {i} and {i - 1} out of order: {prev_dt} > {this_dt}"
        i += 1

    # Note that the above block stops most paging infinite loops quickly
    # ("already seen this dataset id")
    # So we perform this length check in the same method and afterwards.
    if expect_count is not None:
        printable_product_counts = "\n\t".join(
            f"{k}: {v}" for k, v in product_counts.items()
        )
        if isinstance(expect_count, int):
            assert i == expect_count, (
                f"Expected {expect_count} items.\n"
                "Got:\n"
                f"\t{printable_product_counts}"
            )
        else:
            assert product_counts == expect_count


def _iter_items_across_pages(
    client: FlaskClient, url: str
) -> Generator[Dict, None, None]:
    """
    Keep loading "next" pages and yield every stac Item in order
    """
    while url is not None:
        items = get_items(client, url)

        yield from items["features"]
        url = _get_next_href(items)


def test_stac_search_limits(stac_client: FlaskClient):
    # Tell user with error if they request too much.
    large_limit = OUR_DATASET_LIMIT + 1
    rv: Response = stac_client.get(f"/stac/search?" f"&limit={large_limit}")
    assert rv.status_code == 400
    assert b"Max page size" in rv.data

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
                "limit": OUR_PAGE_SIZE,
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

    # Features should include all bands.

    for feature in doc["features"]:
        bands = ["blue", "green", "nir", "red", "swir1", "swir2"]
        with DebugContext(f"feature {feature['id']}"):
            # TODO: These are the same file in a NetCDF. They should probably be one asset?
            assert len(feature["assets"]) == len(
                bands
            ), f"Expected an asset per band, got {repr(feature['assets'])}"
            assert set(feature["assets"].keys()) == set(bands)
            while bands:
                band = bands.pop()
                assert band in feature["assets"]

                band_d = feature["assets"][band]
                assert band_d["roles"] == ["data"]
                assert band_d["eo:bands"] == [{"name": band}]
                # These have no path, so they should be the dataset location itself.
                # (this is a .nc file in reality, but our test data loading creates weird locations)
                assert (
                    band_d["href"] == f'file://example.com/test_dataset/{feature["id"]}'
                )

            # Validate stac item with jsonschema
            validate_item(feature)


def test_stac_collections(stac_client: FlaskClient):
    response = get_json(stac_client, "/stac")

    assert response["id"] == "odc-explorer", "Expected default unconfigured endpoint id"
    assert (
        response["title"] == "Default ODC Explorer instance"
    ), "Expected default unconfigured endpoint title"

    # A child link to each "collection" (product)
    child_links = [r for r in response["links"] if r["rel"] == "child"]
    other_links = [r for r in response["links"] if r["rel"] != "child"]

    # a "self" link.
    assert len(other_links) == 1
    assert other_links[0]["rel"] == "self"

    # All expected products and their dataset counts.
    expected_product_counts = {
        dt.name: _model.STORE.index.datasets.count(product=dt.name)
        for dt in _model.STORE.all_dataset_types()
    }

    found_products = set()
    for child_link in child_links:
        product_name = child_link["title"]
        href = child_link["href"]

        print(f"Loading collection page for {product_name}: {repr(href)}")

        collection_data = get_collection(
            stac_client,
            href,
            # FIXME/research: If there's no datasets in the product, we expect to fail validation
            #                 because we're missing the mandatory spatial/temporal fields
            #                 (there's no "empty polygon" concept I think?)
            validate=expected_product_counts[product_name] > 0
            # Telemetry data also has no spatial properties as it hasn't been processed yet.
            and not product_name.endswith("telemetry_data"),
        )
        assert collection_data["id"] == product_name
        # TODO: assert items, properties, etc.
        found_products.add(product_name)

    # We should have seen all products in the index
    assert found_products == set(expected_product_counts)


def test_stac_collection_items(stac_client: FlaskClient):
    """
    Follow the links to the "high_tide_comp_20p" collection and ensure it includes
    all of our tests data.
    """

    collections = get_json(stac_client, "/stac")
    for link in collections["links"]:
        if link["rel"] == "child" and link["title"] == "high_tide_comp_20p":
            collection_href = link["href"]
            break
    else:
        raise AssertionError("high_tide_comp_20p not found in collection list")

    scene_collection = get_collection(stac_client, collection_href)
    pprint(scene_collection)
    assert scene_collection == {
        "stac_version": "1.0.0-beta.2",
        "id": "high_tide_comp_20p",
        "title": "high_tide_comp_20p",
        "license": "CC-BY-4.0",
        "properties": {},
        "description": "High Tide 20 percentage composites for entire coastline",
        "extent": {
            "spatial": {
                "bbox": [
                    [
                        112.223_058_990_767_51,
                        -43.829_196_553_065_4,
                        153.985_054_424_922_77,
                        -10.237_104_814_250_783,
                    ]
                ]
            },
            "temporal": {
                "interval": [["2008-06-01T00:00:00+00:00", "2008-06-01T00:00:00+00:00"]]
            },
        },
        "links": [
            {
                "href": "http://localhost/collections/high_tide_comp_20p/items",
                "rel": "items",
            }
        ],
        "providers": [],
    }

    item_links = scene_collection["links"][0]["href"]
    validate_items(_iter_items_across_pages(stac_client, item_links), expect_count=306)


def test_stac_item(stac_client: FlaskClient):
    # Load one stac dataset from the test data.
    response = get_item(
        stac_client,
        "http://localhost/collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
    )
    print(repr(response))

    # Our item document can still be improved. This is ensuring changes are deliberate.
    assert response == {
        "stac_version": "1.0.0-beta.2",
        "stac_extensions": ["eo", "projection"],
        "type": "Feature",
        "id": "0c5b625e-5432-4911-9f7d-f6b894e27f3c",
        "bbox": [
            140.03596008297276,
            -32.68883005637166,
            142.6210671177689,
            -30.779953471187625,
        ],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [140.494174472712, -30.779953471187625],
                    [140.48160638713588, -30.786613939351987],
                    [140.47654885652616, -30.803517459008308],
                    [140.26694302361142, -31.554989847530283],
                    [140.11071136692811, -32.10972728807016],
                    [140.05019849367122, -32.32331059968287],
                    [140.03596008297276, -32.374863567950605],
                    [140.04582698730871, -32.37992930113176],
                    [140.09253434030472, -32.38726630955288],
                    [142.19093826112766, -32.68798630718157],
                    [142.19739423481033, -32.68883005637166],
                    [142.20859663812988, -32.688497041665755],
                    [142.21294093862082, -32.68685778341274],
                    [142.6210671177689, -31.092487513703713],
                    [142.6090583939577, -31.083354434650456],
                    [142.585607903412, -31.08001593849131],
                    [140.494174472712, -30.779953471187625],
                ]
            ],
        },
        "properties": {
            "created": "2017-07-11T01:32:22+00:00",
            "datetime": "2017-05-02T00:29:01+00:00",
            "title": "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502",
            "platform": "landsat-7",
            "instruments": ["etm"],
            "landsat:wrs_path": 96,
            "landsat:wrs_row": 82,
            "cubedash:region_code": "96_82",
            "odc:product": "ls7_nbar_scene",
            "proj:epsg": 4326,
        },
        "assets": {
            "1": {
                "eo:bands": [{"name": "1"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B1.tif",
            },
            "2": {
                "eo:bands": [{"name": "2"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B2.tif",
            },
            "3": {
                "eo:bands": [{"name": "3"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B3.tif",
            },
            "4": {
                "eo:bands": [{"name": "4"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B4.tif",
            },
            "5": {
                "eo:bands": [{"name": "5"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B5.tif",
            },
            "7": {
                "eo:bands": [{"name": "7"}],
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/product/"
                "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B7.tif",
            },
            "thumbnail:full": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/browse.fr.jpg",
            },
            "thumbnail:medium": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": "http://localhost/collections/ls7_nbar_scene/items/browse.jpg",
            },
            "checksum:sha1": {
                "type": "text/plain",
                "href": "http://localhost/collections/ls7_nbar_scene/items/package.sha1",
            },
        },
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "href": "http://localhost/collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
            },
            {
                "title": "ODC Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": "http://localhost/dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c.odc-metadata.yaml",
            },
            {
                "title": "ODC Product Overview",
                "rel": "product_overview",
                "type": "text/html",
                "href": "http://localhost/product/ls7_nbar_scene",
            },
            {
                "title": "ODC Dataset Overview",
                "rel": "alternative",
                "type": "text/html",
                "href": "http://localhost/dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
            },
            {
                "rel": "parent",
                "href": "http://localhost/stac/collections/ls7_nbar_scene",
            },
        ],
    }


def assert_stac_extensions(doc: Dict):
    stac_extensions = doc.get("stac_extensions", ())

    for extension_name in stac_extensions:
        assert (
            extension_name in _STAC_EXTENSIONS
        ), f"Unknown stac extension? No schema for {extension_name}"

        _STAC_EXTENSIONS[extension_name].validate(doc)


def assert_item_collection(collection: Dict):
    assert "features" in collection, "No features in collection"
    _ITEM_COLLECTION_SCHEMA.validate(collection)
    assert_stac_extensions(collection)
    validate_items(collection["features"])


def assert_collection(collection: Dict):
    _COLLECTION_SCHEMA.validate(collection)
    assert "features" not in collection
    assert_stac_extensions(collection)

    # Does it have a link to the list of items?
    links = collection["links"]
    assert links, "No links in collection"
    rels = [r["rel"] for r in links]
    # TODO: 'child'? The newer stac examples use that rather than items.
    assert "items" in rels, "Collection has no link to its items"


def validate_item(item: Dict):
    _ITEM_SCHEMA.validate(item)

    # Should be a valid polygon
    assert "geometry" in item, "Item has no geometry"
    assert item["geometry"], "Item has blank geometry"
    with DebugContext(f"Failing shape:\n{pformat(item['geometry'])}"):
        shape = shapely_shape(item["geometry"])
        assert shape.is_valid, f"Item has invalid geometry: {explain_validity(shape)}"
        assert shape.geom_type in (
            "Polygon",
            "MultiPolygon",
        ), "Unexpected type of shape"

    # href should never be blank if present
    # -> The jsonschema enforces href as required, but it's not checking for emptiness.
    #    (and we've had empty ones in previous prototypes)
    for offset, value in research(item, lambda p, k, v: k == "href"):
        viewable_offset = "→".join(map(repr, offset))
        assert value.strip(), f"href has empty value: {repr(viewable_offset)}"

    assert_stac_extensions(item)


def _get_next_href(geojson: Dict) -> Optional[str]:
    hrefs = [link["href"] for link in geojson.get("links", []) if link["rel"] == "next"]
    if not hrefs:
        return None

    assert len(hrefs) == 1, "Multiple next links found: " + ",".join(hrefs)
    [href] = hrefs
    return href
