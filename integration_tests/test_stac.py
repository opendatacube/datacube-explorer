"""
Tests that hit the stac api
"""

import json
import urllib.parse
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from pprint import pformat
from typing import Dict, Generator, Iterable, List, Optional, Union
from urllib.parse import urlsplit
from urllib.request import urlopen

import jsonschema
import pytest
from datacube.utils import is_url, read_documents
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient
from jsonschema import SchemaError
from referencing import Registry, Resource
from shapely.geometry import shape as shapely_shape
from shapely.validation import explain_validity

import cubedash._stac
from cubedash import _model
from integration_tests.asserts import (
    DebugContext,
    assert_matching_eo3,
    get_geojson,
    get_json,
    get_text_response,
)

ALLOW_INTERNET = True

DEFAULT_TZ = tz.gettz("Australia/Darwin")

# Smaller values to ease testing.
OUR_DATASET_LIMIT = 20
OUR_PAGE_SIZE = 4

_SCHEMA_BASE = Path(__file__).parent / "schemas"
_STAC_SCHEMA_BASE = _SCHEMA_BASE / f"stac/{cubedash._stac.STAC_VERSION}"

_SCHEMAS_BY_NAME = defaultdict(list)
for schema_path in _SCHEMA_BASE.rglob("*.json"):
    _SCHEMAS_BY_NAME[schema_path.name].append(schema_path)

METADATA_TYPES = [
    "metadata/eo3_landsat_ard.odc-type.yaml",
    "metadata/eo3_landsat_l1.odc-type.yaml",
    "metadata/eo3_metadata.yaml",
    "metadata/eo_metadata.yaml",
    "metadata/eo_plus.yaml",
    "metadata/landsat_l1_scene.yaml",
    "metadata/qga_eo.yaml",
]
PRODUCTS = [
    "products/dsm1sv10.odc-product.yaml",
    "products/hltc.odc-product.yaml",
    "products/l1_ls8_ga.odc-product.yaml",
    "products/l1_ls5.odc-product.yaml",
    "products/ls5_fc_albers.odc-product.yaml",
    "products/ls5_nbart_albers.odc-product.yaml",
    "products/ls5_nbart_tmad_annual.odc-product.yaml",
    "products/ls5_scenes.odc-product.yaml",
    "products/ls7_nbart_tmad_annual.odc-product.yaml",
    "products/ls7_nbar_albers.odc-product.yaml",
    "products/ls7_nbart_albers.odc-product.yaml",
    "products/ls7_scenes.odc-product.yaml",
    "products/ls8_nbar_albers.odc-product.yaml",
    "products/ls8_nbart_albers.odc-product.yaml",
    "products/ls8_scenes.odc-product.yaml",
    "products/pq_count_summary.odc-product.yaml",
    "products/usgs_ls7e_level1_1.odc-product.yaml",
    "products/wofs_albers.yaml",
    "products/ga_ls8c_ard_3.odc-product.yaml",
]
DATASETS = [
    "datasets/high_tide_comp_20p.yaml.gz",
    # These have very large footprints, as they were unioned from many almost-identical
    # polygons and not simplified. They will trip up postgis if used naively.
    # (postgis gist index has max record size of 8k per entry)
    "datasets/pq_count_summary.yaml.gz",
    "datasets/wofs-albers-sample.yaml.gz",
    "datasets/ga_ls8c_ard_3-sample.yaml",
]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


#######################################
# Helpers
#######################################

# Schema and URL helpers


def explorer_url(offset: str):
    """The public absolute url for this url"""
    return urllib.parse.urljoin("http://localhost/", offset)


def stac_url(offset: str):
    """The public absolute url for this stac url"""
    return urllib.parse.urljoin("http://localhost/stac/", offset)


def read_document(path: Path) -> dict:
    """
    Read and parse exactly one document.
    """
    ds = list(read_documents(path))
    if len(ds) != 1:
        raise ValueError(f"Expected only one document to be in path {path}")

    _, doc = ds[0]
    return doc


def _web_reference(ref: str):
    """
    A reference to a schema via a URL

    eg http://geojson.org/schemas/Features.json'
    """
    if not is_url(ref):
        raise ValueError(f"Expected URL? Got {ref!r}")
    (scheme, netloc, offset, params, query, fragment) = urllib.parse.urlparse(ref)
    # We used `wget -r` to download the remote schemas locally.
    # It puts into hostname/path folders by default. Eg. 'geojson.org/schema/Feature.json'
    path = _SCHEMA_BASE / f"{netloc}{offset}"
    if not path.exists():
        if ALLOW_INTERNET:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(urlopen(ref).read())
        else:
            raise ValueError(
                f"No local copy exists of schema {ref!r}.\n"
                f"\tPerhaps we need to add it to ./update.sh in the tests folder?\n"
                f"\t(looked in {path})"
            )
    return read_document(path)


# Allow schemas to reference other schemas in the same folder.
def _local_reference(schema_location: Path, ref):
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
    raise ValueError(f"Schema reference not found: {ref!r} (within {schema_location})")


def load_validator(schema_location: Path) -> jsonschema.Draft7Validator:
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
    return load_schema_doc(schema, location=schema_location)


def load_schema_doc(
    schema: Dict, location: Union[str, Path]
) -> jsonschema.Draft7Validator:
    try:
        jsonschema.Draft7Validator.check_schema(schema)
    except SchemaError as e:
        raise RuntimeError(f"Invalid schema {location}") from e

    def retrieve(uri: str):
        parsed = urlsplit(uri)
        if parsed.scheme == "https" or parsed.scheme == "http":
            return Resource.from_contents(_web_reference(uri))
        return Resource.from_contents(_local_reference(location, uri))

    return jsonschema.Draft7Validator(schema, registry=Registry(retrieve=retrieve))


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


# Getters


@lru_cache
def get_extension(url: str) -> jsonschema.Draft7Validator:
    if not is_url(url):
        raise ValueError(
            f"stac extensions are now expected to be URLs in 1.0.0. " f"Got {url!r}"
        )
    return load_schema_doc(_web_reference(url), location=url)


def get_collection(client: FlaskClient, url: str, validate=True) -> Dict:
    """
    Get a URL, expecting a valid stac collection document to be there"""
    with DebugContext(f"Requested {url!r}"):
        data = get_json(client, url)
        if validate:
            assert_collection(data)
    return data


def get_items(client: FlaskClient, url: str) -> Dict:
    """
    Get a URL, expecting a valid stac item collection document to be there"""
    with DebugContext(f"Requested {url!r}"):
        data = get_geojson(client, url)
        assert_item_collection(data)
    return data


def get_item(client: FlaskClient, url: str) -> Dict:
    """
    Get a URL, expecting a single valid Stac Item to be there
    """
    with DebugContext(f"Requested {url!r}"):
        data = get_json(client, url)
        validate_item(data)
    return data


def _get_next_href(geojson: Dict) -> Optional[str]:
    hrefs = [link["href"] for link in geojson.get("links", []) if link["rel"] == "next"]
    if not hrefs:
        return None

    assert len(hrefs) == 1, "Multiple next links found: " + ",".join(hrefs)
    [href] = hrefs
    return href


def _iter_items_across_pages(
    client: FlaskClient, url: str
) -> Generator[Dict, None, None]:
    """
    Keep loading "next" pages and yield every Stac Item in order
    """
    while url is not None:
        items = get_items(client, url)

        yield from items["features"]
        url = _get_next_href(items)


# Assertions and validations


def assert_stac_extensions(doc: Dict):
    stac_extensions = doc.get("stac_extensions", ())
    for extension_name in stac_extensions:
        get_extension(extension_name).validate(doc)


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
    assert "geometry" in item, "Item has no geometry field"

    if item["geometry"] is not None:
        with DebugContext(f"Failing shape:\n{pformat(item['geometry'])}"):
            shape = shapely_shape(item["geometry"])
            assert (
                shape.is_valid
            ), f"Item has invalid geometry: {explain_validity(shape)}"
            assert shape.geom_type in (
                "Polygon",
                "MultiPolygon",
            ), "Unexpected type of shape"

    assert_stac_extensions(item)


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
    # __tracebackhide__ = True
    seen_ids = set()
    last_item = None
    i = 0
    product_counts = Counter()
    for item in items:
        id_ = item["id"]
        with DebugContext(f"Invalid item {i}, id {str(id_)!r}"):
            validate_item(item)
        product_counts[item["properties"].get("odc:product", item["collection"])] += 1

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


#######################################
# Tests
#######################################


@pytest.fixture()
def stac_client(client: FlaskClient):
    """
    Get a client with populated data and standard settings
    """
    cubedash._stac.PAGE_SIZE_LIMIT = OUR_DATASET_LIMIT
    cubedash._stac.DEFAULT_PAGE_SIZE = OUR_PAGE_SIZE
    _model.app.config["CUBEDASH_DEFAULT_LICENSE"] = "CC-BY-4.0"
    return client


# Page requests


def test_stac_loading_all_pages(stac_client: FlaskClient):
    # An unconstrained search returning every dataset.
    # It should return every dataset in order with no duplicates.
    all_items = _iter_items_across_pages(stac_client, "/stac/search")
    validate_items(
        all_items,
        expect_count={
            "high_tide_comp_20p": 306,
            "pq_count_summary": 20,
            "wofs_albers": 11,
            "ls8_nbar_scene": 7,
            "ls8_level1_scene": 7,
            "ls8_nbart_scene": 7,
            "ls8_pq_legacy_scene": 7,
            "ls8_nbart_albers": 7,
            "ls8_satellite_telemetry_data": 7,
            "ls7_satellite_telemetry_data": 4,
            "ls7_nbart_albers": 4,
            "ls7_nbart_scene": 4,
            "ls7_nbar_scene": 4,
            "ls7_pq_legacy_scene": 4,
            "ls7_level1_scene": 4,
            "dsm1sv10": 1,
            "ga_ls8c_ard_3": 21,
        },
    )

    # A constrained search within a bounding box.
    # It should return matching datasets in order with no duplicates.
    all_items = _iter_items_across_pages(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[114, -33, 153, -10]"
            "&datetime=2017-04-16T01:12:16/2017-05-10T00:24:21"
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


def test_huge_page_request(stac_client: FlaskClient):
    """Return an error if they try to request beyond max-page-size limit"""
    error_message_json = get_json(
        stac_client,
        f"/stac/search?&limit={OUR_DATASET_LIMIT + 1}",
        expect_status_code=400,
    )
    assert error_message_json == {
        "code": 400,
        "name": "Bad Request",
        "description": f"Max page size is {OUR_DATASET_LIMIT}. Use the next links instead of a large limit.",
    }


def test_returns_404s(stac_client: FlaskClient):
    """
    We should get 404 messages, not exceptions, for missing things.

    (and stac errors are expected in json)
    """

    def expect_404(url: str, message_contains: str = None):
        __tracebackhide__ = True
        data = get_json(stac_client, url, expect_status_code=404)
        if message_contains and message_contains not in data.get("description", ""):
            raise AssertionError(
                f"Expected {message_contains!r} in description of response {data!r}"
            )

    # Product
    expect_404(
        "/stac/collections/does_not_exist", message_contains="Unknown collection"
    )

    # Product items
    expect_404(
        "/stac/collections/does_not_exist/items",
        message_contains="Product 'does_not_exist' not found",
    )

    # Dataset
    wrong_dataset_id = "37296b9a-e6ec-4bfd-ab80-cc32902429d1"
    expect_404(
        f"/stac/collections/does_not_exist/items/{wrong_dataset_id}",
        message_contains="No dataset found",
    )
    # Should be a 404, not a server or postgres error.
    get_text_response(
        stac_client,
        "/stac/collections/does_not_exist/items/not-a-uuid",
        expect_status_code=404,
    )


@pytest.mark.parametrize(
    ("url", "redirect_to_url"),
    [
        (
            "/collections/ls7_nbar_scene",
            "/stac/collections/ls7_nbar_scene",
        ),
        (
            "/collections/ls7_nbar_scene/items",
            "/stac/collections/ls7_nbar_scene/items",
        ),
        (
            # Maintains extra query parameters in the redirect
            "/collections/ls7_nbar_scene/items"
            "?datetime=2000-01-01/2000-01-01&bbox=-48.206,-14.195,-45.067,-12.272",
            "/stac/collections/ls7_nbar_scene/items"
            "?datetime=2000-01-01/2000-01-01&bbox=-48.206,-14.195,-45.067,-12.272",
        ),
        (
            "/collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
            "/stac/collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
        ),
    ],
)
def test_legacy_redirects(stac_client: FlaskClient, url: str, redirect_to_url: str):
    resp: Response = stac_client.get(url, follow_redirects=False)
    assert resp.location == redirect_to_url, (
        f"Expected {url} to be redirected to:\n"
        f"             {redirect_to_url}\n"
        f"  instead of {resp.location}"
    )


# Stac validation


def test_stac_links(stac_client: FlaskClient):
    """Check that root contains all expected links"""
    response = get_json(stac_client, "/stac")
    _CATALOG_SCHEMA.validate(response)

    assert response["id"] == "odc-explorer", "Expected default unconfigured endpoint id"
    assert (
        response["title"] == "Default ODC Explorer instance"
    ), "Expected default unconfigured endpoint title"

    # A child link to each "collection" (product)
    child_links = [r for r in response["links"] if r["rel"] == "child"]
    other_links = [r for r in response["links"] if r["rel"] != "child"]
    assert other_links == [
        {
            "rel": "root",
            "href": "http://localhost/stac",
            "type": "application/json",
            "title": "Default ODC Explorer instance",
        },
        {
            "rel": "self",
            "href": "http://localhost/stac",
            "type": "application/json",
        },
        {
            "rel": "children",
            "href": "http://localhost/stac/collections",
            "type": "application/json",
            "title": "Collections",
        },
        {
            "rel": "search",
            "href": "http://localhost/stac/search",
            "type": "application/json",
            "title": "Item Search",
        },
        {
            "rel": "http://www.opengis.net/def/rel/ogc/1.0/queryables",
            "href": "http://localhost/stac/queryables",
            "type": "application/json",
            "title": "Queryables",
        },
    ]

    # All expected products and their dataset counts.
    expected_product_counts = {
        dt.name: _model.STORE.index.datasets.count(product=dt.name)
        for dt in _model.STORE.all_dataset_types()
    }

    found_collection_ids = set()
    for child_link in child_links:
        product_name: str = child_link["title"]
        href: str = child_link["href"]
        # ignore child links corresponding to catalogs
        if "catalogs" not in href:
            print(f"Loading collection page for {product_name}: {href!r}")

            collection_data = get_collection(stac_client, href, validate=True)
            assert collection_data["id"] == product_name
            # TODO: assert items, properties, etc.

            found_collection_ids.add(product_name)

    # We should have seen all products in the index
    assert sorted(found_collection_ids) == sorted(tuple(expected_product_counts.keys()))


def test_arrivals_page_validation(stac_client: FlaskClient):
    # Do the virtual 'arrivals' catalog and items validate?
    response = get_json(stac_client, "/stac/catalogs/arrivals")
    _CATALOG_SCHEMA.validate(response)

    assert response["id"] == "arrivals"
    assert response["title"] == "Dataset Arrivals"

    [items_page_url] = [i["href"] for i in response["links"] if i["rel"] == "items"]

    # Get and validate items.
    items = get_items(stac_client, items_page_url)
    # Sanity check.
    assert len(items["features"]) == OUR_PAGE_SIZE


def test_stac_collection(stac_client: FlaskClient):
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

    scene_collection = get_collection(stac_client, collection_href, validate=False)
    # HACK: Heavy handed bypass for pytest approximate
    import copy

    proxy_scene_collection = copy.deepcopy(scene_collection)
    proxy_scene_collection["extent"]["spatial"]["bbox"][0] = [
        pytest.approx(p, abs=0.001)
        for p in scene_collection["extent"]["spatial"]["bbox"][0]
    ]

    assert proxy_scene_collection == {
        "stac_version": "1.0.0",
        "type": "Collection",
        "id": "high_tide_comp_20p",
        "title": "high_tide_comp_20p",
        "license": "CC-BY-4.0",
        "description": "High Tide 20 percentage composites for entire coastline",
        "extent": {
            "spatial": {
                "bbox": [
                    [
                        pytest.approx(112.223_058_990_767_51, abs=0.001),
                        pytest.approx(-43.829_196_553_065_4, abs=0.001),
                        pytest.approx(153.985_054_424_922_77, abs=0.001),
                        pytest.approx(-10.237_104_814_250_783, abs=0.001),
                    ]
                ]
            },
            "temporal": {
                "interval": [["2008-06-01T00:00:00Z", "2008-06-01T00:00:00Z"]]
            },
        },
        "links": [
            {
                "rel": "root",
                "href": "http://localhost/stac",
                "type": "application/json",
                "title": "Default ODC Explorer instance",
            },
            {
                "rel": "self",
                "href": stac_url("collections/high_tide_comp_20p"),
            },
            {
                "rel": "items",
                "href": stac_url("collections/high_tide_comp_20p/items"),
            },
            {
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/queryables",
                "href": stac_url("collections/high_tide_comp_20p/queryables"),
            },
            {
                "rel": "child",
                "href": stac_url("catalogs/high_tide_comp_20p/2008-6"),
            },
        ],
        # "providers": [], // FIXME: These disappeared somewhere along the way ?
        # "stac_extensions": [],
    }
    # HACK: Make things float again
    assert_collection(scene_collection)
    for link in scene_collection["links"]:
        if link["rel"] == "items":
            item_links = link["href"]
            break
    validate_items(_iter_items_across_pages(stac_client, item_links), expect_count=306)


# @pytest.mark.xfail()
def test_stac_item(stac_client: FlaskClient, odc_test_db):
    # Load one stac dataset from the test data.

    dataset_uri = (
        "file:///g/data/rs0/scenes/ls7/2017/05/output/nbar/"
        "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502/ga-metadata.yaml"
    )
    odc_test_db.index.datasets.add_location(
        "0c5b625e-5432-4911-9f7d-f6b894e27f3c", dataset_uri
    )

    response = get_item(
        stac_client,
        stac_url(
            "collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c"
        ),
    )

    def dataset_url(s: str):
        return (
            f"file:///g/data/rs0/scenes/ls7/2017/05/output/nbar/"
            f"LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502/{s}"
        )

    # Our item document can still be improved.
    # This is ensuring changes are deliberate.
    expected = {
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        ],
        "type": "Feature",
        "collection": "ls7_nbar_scene",
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
            "created": "2017-07-11T01:32:22Z",
            "datetime": "2017-05-02T00:29:01Z",
            "title": "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502",
            "platform": "landsat-7",
            "instruments": ["etm"],
            "landsat:wrs_path": 96,
            "landsat:wrs_row": 82,
            "cubedash:region_code": "96_82",
            "proj:epsg": 4326,
        },
        "assets": {
            "1": {
                "title": "1",
                "eo:bands": [{"name": "1"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B1.tif"
                ),
            },
            "2": {
                "title": "2",
                "eo:bands": [{"name": "2"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B2.tif"
                ),
            },
            "3": {
                "title": "3",
                "eo:bands": [{"name": "3"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B3.tif"
                ),
            },
            "4": {
                "title": "4",
                "eo:bands": [{"name": "4"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B4.tif"
                ),
            },
            "5": {
                "title": "5",
                "eo:bands": [{"name": "5"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B5.tif"
                ),
            },
            "7": {
                "title": "7",
                "eo:bands": [{"name": "7"}],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": dataset_url(
                    "product/LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502_B7.tif"
                ),
            },
            "thumbnail:full": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": dataset_url("browse.fr.jpg"),
            },
            "thumbnail:medium": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": dataset_url("browse.jpg"),
            },
            "checksum:sha1": {
                "type": "text/plain",
                "href": dataset_url("package.sha1"),
                "roles": ["metadata"],
            },
        },
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "href": stac_url(
                    "collections/ls7_nbar_scene/items/0c5b625e-5432-4911-9f7d-f6b894e27f3c"
                ),
            },
            {
                "title": "ODC Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": explorer_url(
                    "dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c.odc-metadata.yaml"
                ),
            },
            {
                "rel": "collection",
                "href": stac_url("collections/ls7_nbar_scene"),
            },
            {
                "title": "ODC Product Overview",
                "rel": "product_overview",
                "type": "text/html",
                "href": explorer_url("product/ls7_nbar_scene"),
            },
            {
                "title": "ODC Dataset Overview",
                "rel": "alternative",
                "type": "text/html",
                "href": explorer_url("dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c"),
            },
            {
                "rel": "canonical",
                "type": "text/yaml",
                "href": dataset_uri,
            },
            {
                "title": "Default ODC Explorer instance",
                "rel": "root",
                "type": "application/json",
                "href": "http://localhost/stac",
            },
        ],
    }
    assert_matching_eo3(response, expected)


# Search tests


def test_stac_search_limits(stac_client: FlaskClient):
    # Tell user with error if they request too much.
    large_limit = OUR_DATASET_LIMIT + 1
    rv: Response = stac_client.get(f"/stac/search?&limit={large_limit}")
    assert rv.status_code == 400
    assert b"Max page size" in rv.data

    # Without limit, it should use the default page size
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[114, -33, 153, -10]"
            "&datetime=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    assert len(geojson.get("features")) == OUR_PAGE_SIZE


def test_stac_search_zero(stac_client: FlaskClient):
    # Zero limit is a valid query
    zero_limit = 0
    rv: Response = stac_client.get(f"/stac/search?&limit={zero_limit}")
    assert rv.status_code == 200


def test_stac_includes_total(stac_client: FlaskClient):
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "&bbox=[114, -33, 153, -10]"
            "&datetime=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    assert geojson.get("numberMatched") == 72


def test_next_link(stac_client: FlaskClient):
    # next link should return next page of results
    geojson = get_items(
        stac_client,
        ("/stac/search?" "collections=ga_ls8c_ard_3,ls7_nbart_albers"),
    )
    assert geojson.get("numberMatched") > len(geojson.get("features"))

    next_link = _get_next_href(geojson)
    assert next_link is not None
    next_link = next_link.replace("http://localhost", "")

    next_page = get_items(stac_client, next_link)
    assert next_page.get("numberMatched") == geojson.get("numberMatched")
    assert next_page["context"]["page"] == 1


def test_stac_search_by_ids(stac_client: FlaskClient):
    def geojson_feature_ids(d: Dict) -> List[str]:
        return sorted(d.get("id") for d in geojson.get("features", {}))

    # Can filter to an empty list. Nothing returned.
    geojson = get_items(
        stac_client,
        "/stac/search?&collection=ls7_nbart_albers&ids=",
    )
    assert len(geojson.get("features")) == 0

    # Can request one dataset
    geojson = get_items(
        stac_client,
        "/stac/search?ids=cab65f3f-bb38-4605-9d6a-eff5ea786376",
    )
    assert geojson_feature_ids(geojson) == ["cab65f3f-bb38-4605-9d6a-eff5ea786376"]

    # Other params are included when ids is specified (Matching the newer Stac API spec)
    geojson = get_items(
        stac_client,
        (
            "/stac/search?datetime=1975-01-01/1976-01-01&ids=cab65f3f-bb38-4605-9d6a-eff5ea786376"
        ),
    )
    assert geojson_feature_ids(geojson) == []

    # Can request multiple datasets
    geojson = get_items(
        stac_client,
        (
            "/stac/search?&collection=ls7_nbart_albers"
            "&ids=cab65f3f-bb38-4605-9d6a-eff5ea786376, "
            "306a5281-02df-4d27-b1eb-b1cda81a35e3,"
            "696c2481-700e-4fec-b438-01396430a688"
        ),
    )
    assert geojson_feature_ids(geojson) == [
        "306a5281-02df-4d27-b1eb-b1cda81a35e3",
        "696c2481-700e-4fec-b438-01396430a688",
        "cab65f3f-bb38-4605-9d6a-eff5ea786376",
    ]
    assert geojson["numberMatched"] == 3

    # Can filter using ids that don't exist.
    geojson = get_items(
        stac_client,
        "/stac/search?&ids=7afd04ad-6080-4ee8-a280-f64853b399ca",
    )
    assert len(geojson.get("features")) == 0

    # Old JSON-like syntax should be supported for now.
    # (Sat-api and the old code used this?)
    geojson = get_items(
        stac_client,
        '/stac/search?ids=["cab65f3f-bb38-4605-9d6a-eff5ea786376"]',
    )
    assert geojson_feature_ids(geojson) == ["cab65f3f-bb38-4605-9d6a-eff5ea786376"]

    # HTTP-Bad-Request should be returned when not a valid uuid format
    # TODO: perhaps ideally this should be treated as an unknown id... but this is at
    #       least better than the old Postgres error
    error_message_json = get_json(
        stac_client,
        ("/stac/search?&collection=ls7_nbart_albers" "&ids=7a[-fd04ad[-"),
        expect_status_code=400,
    )
    assert error_message_json["name"] == "Bad Request"


def test_stac_search_by_intersects(stac_client: FlaskClient):
    """
    We have the polygon for region 16,-33.

    Lets do an 'intersects' GeoJSON polygon search.

    ... and we should only get the one dataset in that region.
    """
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "wofs_albers",
                # Does it intersect the region 16_-33 geojson?
                "intersects": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [148.72461549555535, -29.391767542831246],
                            [148.65670313473439, -28.94873478955442],
                            [148.58964759480034, -28.507789084391693],
                            [148.7948853750446, -28.48453447807445],
                            [148.99881736640228, -28.461136468290533],
                            [149.30557553957937, -28.425393085381657],
                            [149.61378885630688, -28.388816565923616],
                            [149.75688045708, -29.27179937570222],
                            [149.3116110723344, -29.32445502185201],
                            [149.2401198887219, -29.332780925452646],
                            [148.9851375703057, -29.362187515640972],
                            [148.72461549555535, -29.391767542831246],
                        ]
                    ],
                },
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200, f"Error response: {rv.data}"
    doc = rv.json

    features = doc.get("features")

    # We only returned the feature of that region code?
    assert len(features) == 1
    assert features[0]["properties"]["cubedash:region_code"] == "16_-33"


def test_stac_search_by_intersects_paging(stac_client: FlaskClient):
    """
    When we search by 'intersects', the next page link should use a correct POST request.

    (Stac only supports 'intersects' for POST requests.)
    """
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                # Roughly the whole region of Australia.
                "intersects": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [112.1484375, -43.32517767999294],
                            [153.6328125, -43.32517767999294],
                            [153.6328125, -8.407168163601076],
                            [112.1484375, -8.407168163601076],
                            [112.1484375, -43.32517767999294],
                        ]
                    ],
                },
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200, f"Error response: {rv.data}"
    doc = rv.json

    # We got a whole page of features.
    assert len(doc.get("features")) == OUR_PAGE_SIZE

    # And a POST link to the next page.
    [next_link] = [link for link in doc.get("links", []) if link["rel"] == "next"]

    assert next_link == {
        "rel": "next",
        "title": "Next page of Items",
        "type": "application/geo+json",
        "method": "POST",
        "href": "http://localhost/stac/search",
        # Tell the client to merge with their original params, but set a new offset.
        "merge": True,
        "body": {"_o": OUR_PAGE_SIZE},
    }


def test_stac_search_collections(stac_client: FlaskClient):
    """Can you query a list of multiple collections?"""

    # Get all in one collection
    geojson = get_items(
        stac_client,
        ("/stac/search?" "&collections=ls7_nbart_scene" "&limit=20"),
    )
    assert len(geojson.get("features")) == 4

    # Get all the datasets for two collections
    geojson = get_items(
        stac_client,
        ("/stac/search?" "&collections=ls7_nbart_scene,ls7_nbar_scene" "&limit=20"),
    )
    # Four datasets each.
    assert len(geojson.get("features")) == 8
    returned_feature_ids = sorted(f["id"] for f in geojson["features"])
    assert returned_feature_ids == [
        "0c5b625e-5432-4911-9f7d-f6b894e27f3c",
        "1649d316-7713-461e-b178-d41cc967e544",
        "27482638-1208-427f-8420-6eecf41e8d0e",
        "39543725-63cc-4e51-a0a1-b07fb66edfd9",
        "47017789-c42d-451e-b268-591309a783c7",
        "57c94973-7ab9-45db-a0b8-a51daf180cc2",
        "74b8b54b-3985-4892-afd5-0efacbe59d2f",
        "7da7db97-5ab9-44bf-b326-8a75f33fe2be",
    ]

    # An empty URL parameter means it's unspecified.
    # (its doesn't mean match-the-empty-list!)
    geojson = get_items(
        stac_client,
        ("/stac/search?" "&collections=" "&limit=20"),
    )
    assert len(geojson.get("features")) > 0


def test_stac_search_bounds(stac_client: FlaskClient):
    # Outside the box there should be no results
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "&bbox=20,-5,25,10"
            "&datetime=2017-04-16T01:12:16/2017-05-10T00:24:21"
        ),
    )
    assert len(geojson.get("features")) == 0

    # Search a whole-day for a scene
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "collections=ls7_nbar_scene"
            # Legacy json format of array
            "&bbox=[114, -33, 153, -10]"
            "&datetime=2017-04-20"
        ),
    )
    assert len(geojson.get("features")) == 1

    # Search a whole-day on an empty day.
    geojson = get_items(
        stac_client,
        (
            "/stac/search?"
            "collections=ls7_nbar_scene"
            "&bbox=114.0, -33.0, 153.0, -10.0"
            "&datetime=2017-04-22"
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
                "_full": True,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    assert len(doc.get("features")) == OUR_PAGE_SIZE
    # We requested the full dataset, so band assets etc should be included.
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
                "_full": True,
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
            ), f"Expected an asset per band, got {feature['assets']!r}"
            assert set(feature["assets"].keys()) == set(bands)
            while bands:
                band = bands.pop()
                assert band in feature["assets"]

                band_d = feature["assets"][band]
                assert band_d["roles"] == ["data"]
                assert band_d["eo:bands"] == [{"name": band}]
                # These have no path, so they should be the dataset location itself with a layer.
                # (this is a .nc file in reality, but our test data loading creates weird locations)
                assert (
                    band_d["href"] == f'file://example.com/test_dataset/{feature["id"]}'
                )

            # Validate stac item with jsonschema
            validate_item(feature)


def test_stac_query_extension(stac_client: FlaskClient):
    query = {"properties.dea:dataset_maturity": dict(eq="nrt")}
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "query": query,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    assert len(doc.get("features")) == 1
    assert doc["features"][0]["properties"]["dea:dataset_maturity"] == "nrt"


def test_stac_fields_extension(stac_client: FlaskClient):
    fields = {"include": ["properties.dea:dataset_maturity"]}
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "fields": fields,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    assert doc.get("features")
    keys = set(doc["features"][0].keys())
    assert {
        "id",
        "type",
        "geometry",
        "bbox",
        "links",
        "assets",
        "properties",
        "stac_version",
        "stac_extensions",
    } == keys
    properties = doc["features"][0]["properties"]
    assert {"datetime", "dea:dataset_maturity"} == set(properties.keys())

    fields = {"exclude": ["assets.thumbnail:nbart"]}
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "fields": fields,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    keys = set(doc["features"][0].keys())
    assert "collection" in keys
    properties = doc["features"][0]["assets"]
    assert "thumbnail:nbart" not in set(properties.keys())

    # should we do an invalid field as well?


def test_stac_sortby_extension(stac_client: FlaskClient):
    sortby = [{"field": "properties.datetime", "direction": "asc"}]
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "sortby": sortby,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    assert doc.get("features")
    for i in range(1, len(doc["features"])):
        assert (
            doc["features"][i - 1]["properties"]["datetime"]
            < doc["features"][i]["properties"]["datetime"]
        )

    sortby = [{"field": "properties.datetime", "direction": "desc"}]
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "sortby": sortby,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    for i in range(1, len(doc["features"])):
        assert (
            doc["features"][i - 1]["properties"]["datetime"]
            > doc["features"][i]["properties"]["datetime"]
        )


def test_stac_filter_extension(stac_client: FlaskClient):
    filter_cql = {
        "op": "and",
        "args": [
            {
                "op": "<>",
                "args": [{"property": "properties.dea:dataset_maturity"}, "final"],
            },
            {"op": ">=", "args": [{"property": "properties.eo:cloud_cover"}, float(2)]},
        ],
    }
    rv: Response = stac_client.post(
        "/stac/search",
        data=json.dumps(
            {
                "product": "ga_ls8c_ard_3",
                "time": "2022-01-01T00:00:00/2022-12-31T00:00:00",
                "limit": OUR_DATASET_LIMIT,
                "_full": True,
                "filter": filter_cql,
            }
        ),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    assert rv.status_code == 200
    doc = rv.json
    features = doc.get("features")
    assert len(features) == 2
    ids = [f["id"] for f in features]
    assert "fc792b3b-a685-4c0f-9cf6-f5257f042c64", (
        "192276c6-8fa4-46a9-8bc6-e04e157974b9" in ids
    )
