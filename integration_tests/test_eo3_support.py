from datetime import datetime
from pathlib import Path
from pprint import pprint
from textwrap import dedent
from typing import Dict
from uuid import UUID

import pytest
from datacube.index import Index
from datacube.utils import parse_time
from dateutil import tz
from dateutil.tz import tzutc
from flask import Response
from flask.testing import FlaskClient
from geoalchemy2.shape import to_shape
from ruamel import yaml
from ruamel.yaml import YAML

from cubedash import _utils
from cubedash.summary import SummaryStore, _extents
from cubedash.warmup import find_examples_of_all_public_urls
from integration_tests.asserts import assert_matching_eo3
from integration_tests.test_pages_render import assert_all_urls_render
from integration_tests.test_stac import get_item, get_items

TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_EO3_DATASET_L1 = (
    TEST_DATA_DIR / "LT05_L1TP_113081_19880330_20170209_01_T1.odc-metadata.yaml"
)
TEST_EO3_DATASET_ARD = (
    TEST_DATA_DIR / "ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final.odc-metadata.yaml"
)


@pytest.fixture(scope="module")
def eo3_index(module_dea_index: Index, dataset_loader):
    def _add_from_dir(
        path: Path, expected_product_name: str, expected_dataset_count: int
    ):
        """Add any product definitions and datasets from the given directory."""

        for product in path.glob("*.odc-product.yaml"):
            module_dea_index.products.add_document(yaml.load(product.open()))
        loaded = 0
        for dataset in path.glob("*.odc-metadata.yaml"):
            loaded += dataset_loader(
                expected_product_name,
                dataset,
            )
        assert loaded == expected_dataset_count

    loaded = dataset_loader(
        "usgs_ls5t_level1_1",
        TEST_EO3_DATASET_L1,
    )
    assert loaded == 1

    loaded = dataset_loader(
        "ga_ls5t_ard_3",
        TEST_EO3_DATASET_ARD,
    )
    assert loaded == 1

    _add_from_dir(
        TEST_DATA_DIR / "gm_s2_semiannual",
        expected_product_name="gm_s2_semiannual_lowres",
        expected_dataset_count=1,
    )

    # We need postgis and some support tables (eg. srid lookup).
    store = SummaryStore.create(module_dea_index)
    store.drop_all()
    store.init(grouping_epsg_code=3577)

    return module_dea_index


def test_eo3_extents(eo3_index: Index):
    """
    Do we extract the elements of an EO3 extent properly?

    (ie. not the older grid_spatial definitions)
    """
    [dataset_extent_row] = _extents.get_sample_dataset("ga_ls5t_ard_3", index=eo3_index)
    pprint(dataset_extent_row)

    assert dataset_extent_row["id"] == UUID("5b2f2c50-e618-4bef-ba1f-3d436d9aed14")

    # On older products, the center time was calculated from the range.
    # But on EO3 we have a singular 'datetime' to use directly.
    assert dataset_extent_row["center_time"] == datetime(
        1988, 3, 30, 1, 41, 16, 892044, tzinfo=tz.tzutc()
    )

    assert dataset_extent_row["creation_time"] == datetime(
        2020, 6, 5, 7, 15, 26, 599544, tzinfo=tz.tzutc()
    )
    assert (
        dataset_extent_row["dataset_type_ref"]
        == eo3_index.products.get_by_name("ga_ls5t_ard_3").id
    )

    # This should be the geometry field of eo3, not the max/min bounds
    # that eo1 compatibility adds within `grid_spatial`.
    footprint = to_shape(dataset_extent_row["footprint"])
    assert footprint.__geo_interface__ == {
        "type": "Polygon",
        "coordinates": (
            (
                (271725.0, -3248955.0),
                (271545.69825676165, -3249398.5651073675),
                (269865.69825676165, -3257048.5651073675),
                (260385.671713869, -3301028.687185048),
                (243345.665287001, -3380468.71711744),
                (234975.6600318394, -3419708.7417040393),
                (233985.0, -3424865.9692269894),
                (233985.0, -3427879.8526289104),
                (238960.4382844724, -3428684.6511509297),
                (426880.46102441975, -3457454.6546404576),
                (427870.50583861716, -3457604.6614651266),
                (428083.1938027047, -3457585.2522918927),
                (428204.3403041278, -3457251.2567206817),
                (465584.3403041278, -3281961.2567206817),
                (466034.48616560805, -3279560.5286560515),
                (466004.93355473573, -3279073.0044296845),
                (465859.7539769853, -3279015.379066476),
                (461689.60011989495, -3278355.3547828994),
                (271725.0, -3248955.0),
            ),
        ),
    }
    assert footprint.is_valid, "Created footprint is not a valid geometry"
    assert (
        dataset_extent_row["footprint"].srid == 32650
    ), "Expected epsg:32650 within the footprint geometry"

    assert dataset_extent_row["region_code"] == "113081"
    assert dataset_extent_row["size_bytes"] is None


def test_eo3_dateless_extents(eo3_index: Index):
    """
    Can we support datasets with no datetime field?

    (Stac makes it optional if you have a start/end date)
    """
    [dataset_extent_row] = _extents.get_sample_dataset(
        "gm_s2_semiannual_lowres", index=eo3_index
    )
    pprint(dataset_extent_row)

    assert dataset_extent_row["id"] == UUID("856e45bf-cd50-5a5a-b1cd-12b85df99b24")

    # Since it has no datetime, the chosen one should default to the start
    time_record: datetime = dataset_extent_row["center_time"]
    assert time_record.astimezone(tz.tzutc()) == datetime(
        2017, 7, 1, 0, 0, tzinfo=tz.tzutc()
    )

    # Dataset has no creation time, but will fall back to index time.
    assert dataset_extent_row["creation_time"] is not None
    # ... and no region code either. We do nothing.
    assert dataset_extent_row["region_code"] is None


def test_location_sampling(eo3_index: Index):
    summary_store = SummaryStore.create(eo3_index)

    assert summary_store.product_location_samples("ls8_nbar_albers") == []


def test_eo3_doc_download(eo3_index: Index, client: FlaskClient):
    response: Response = client.get(
        "/dataset/9989545f-906d-5090-a38e-cdbfbfc1afca.odc-metadata.yaml"
    )
    text = response.data.decode("utf-8")
    assert response.status_code == 200, text

    # Check beginning of doc matches expected.
    expected = dedent(
        """\
        ---
        # Dataset
        # url: http://localhost/dataset/9989545f-906d-5090-a38e-cdbfbfc1afca.odc-metadata.yaml
        $schema: https://schemas.opendatacube.org/dataset
        id: 9989545f-906d-5090-a38e-cdbfbfc1afca
    """
    )
    assert text[: len(expected)] == expected


def test_undo_eo3_doc_compatibility(eo3_index: Index):
    """
    ODC adds compatibility fields on index. Check that our undo-method
    correctly creates an indentical document to the original.
    """

    # Get our EO3 ARD document that was indexed.
    indexed_dataset = eo3_index.datasets.get(
        UUID("5b2f2c50-e618-4bef-ba1f-3d436d9aed14"), include_sources=True
    )
    indexed_doc = with_parsed_datetimes(indexed_dataset.metadata_doc)

    # Undo the changes.
    _utils.undo_eo3_compatibility(indexed_doc)

    # The lineage should have been flattened to EO3-style
    assert indexed_doc["lineage"] == {
        "level1": ["9989545f-906d-5090-a38e-cdbfbfc1afca"]
    }

    # And does our original, pre-indexed document match exactly?
    with TEST_EO3_DATASET_ARD.open("r") as f:
        raw_doc = YAML(typ="safe", pure=True).load(f)

    assert (
        indexed_doc == raw_doc
    ), "Document does not match original after undoing compatibility fields."


def with_parsed_datetimes(v: Dict, name=""):
    """
    All date fields in eo3 metadata have names ending in 'datetime'. Return a doc
    with all of these fields parsed as actual dates.

    (they are convertered to strings on datacube index and other json-ification)
    """
    if not v:
        return v

    if name.endswith("datetime"):
        dt = parse_time(v)
        # Strip/normalise timezone to match default yaml.load()
        if dt.tzinfo:
            dt = dt.astimezone(tzutc()).replace(tzinfo=None)
        return dt
    elif isinstance(v, dict):
        return {k: with_parsed_datetimes(v, name=k) for k, v in v.items()}
    elif isinstance(v, list):
        return [with_parsed_datetimes(i) for i in v]

    return v


def test_all_eo3_pages_render(eo3_index: Index, client: FlaskClient):
    """
    Do all expected URLS render with HTTP OK response with our normal eo3 test data?
    """
    assert_all_urls_render(find_examples_of_all_public_urls(eo3_index), client)


def test_can_search_eo3_items(eo3_index, client: FlaskClient):
    """
    Searching returns lightweight item records, so the conversion code is different.
    """
    # Lightweight records...
    geojson = get_items(
        client,
        "http://localhost/stac/collections/ga_ls5t_ard_3/items?_full=false",
    )
    assert len(geojson.get("features")) == 1
    assert "gqa:abs_iterative_mean_xy" not in geojson["features"][0]["properties"]

    # .... And full records
    geojson = get_items(
        client,
        "http://localhost/stac/collections/ga_ls5t_ard_3/items?_full=True",
    )
    assert len(geojson.get("features")) == 1
    assert geojson["features"][0]["properties"][
        "gqa:abs_iterative_mean_xy"
    ] == pytest.approx(0.37)


def test_eo3_stac_item(eo3_index, client: FlaskClient):
    # Load one stac dataset from the test data.
    response = get_item(
        client,
        "http://localhost/stac/collections/ga_ls5t_ard_3/items/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
    )

    # Our item document can still be improved. This is ensuring changes are deliberate.
    expected = {
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
            "https://stac-extensions.github.io/view/v1.0.0/schema.json",
        ],
        "type": "Feature",
        "id": "5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
        "collection": "ga_ls5t_ard_3",
        "bbox": [
            114.21535558993006,
            -31.250437923368555,
            116.64907638404333,
            -29.349050663163574,
        ],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [114.64871391815366, -29.349050663163574],
                    [114.64677639402194, -29.353018115334343],
                    [114.62788289920915, -29.421697120964225],
                    [114.52054747332271, -29.816476622735603],
                    [114.32509273545551, -30.52916962535211],
                    [114.22784148339491, -30.88102334197938],
                    [114.21614357649473, -30.927281454147938],
                    [114.21535558993006, -30.954444432155046],
                    [114.26718123280367, -30.962809472233833],
                    [116.23208607366821, -31.24902286234458],
                    [116.24247211235263, -31.250437923368555],
                    [116.24470706101778, -31.250275955085034],
                    [116.24600323189877, -31.247270084969557],
                    [116.64434947756361, -29.667191340115387],
                    [116.64907638404333, -29.64553738362414],
                    [116.64878632941206, -29.64113670409086],
                    [116.64728826989541, -29.640612658559103],
                    [116.60422943060024, -29.63453443240782],
                    [114.64871391815366, -29.349050663163574],
                ]
            ],
        },
        "properties": {
            "datetime": "1988-03-30T01:41:16.892044Z",
            "created": "2020-06-05T07:15:26.599544Z",
            "title": "ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final",
            "platform": "landsat-5",
            "instruments": ["tm"],
            "gsd": 30.0,
            "start_datetime": "1988-03-30T01:41:03.171855Z",
            "end_datetime": "1988-03-30T01:41:30.539592Z",
            "cubedash:region_code": "113081",
            "dea:dataset_maturity": "final",
            "eo:cloud_cover": 0.23252452200636467,
            "fmask:clear": 69.41254395960313,
            "fmask:cloud": 0.23252452200636467,
            "fmask:cloud_shadow": 0.16922594313723835,
            "fmask:snow": 0.0,
            "fmask:water": 30.18570557525327,
            "gqa:abs_iterative_mean_x": 0.27,
            "gqa:abs_iterative_mean_xy": 0.37,
            "gqa:abs_iterative_mean_y": 0.25,
            "gqa:abs_x": 0.43,
            "gqa:abs_xy": 0.59,
            "gqa:abs_y": 0.41,
            "gqa:cep90": 0.69,
            "gqa:iterative_mean_x": -0.15,
            "gqa:iterative_mean_xy": 0.21,
            "gqa:iterative_mean_y": 0.15,
            "gqa:iterative_stddev_x": 0.31,
            "gqa:iterative_stddev_xy": 0.41,
            "gqa:iterative_stddev_y": 0.27,
            "gqa:mean_x": -0.1,
            "gqa:mean_xy": 0.2,
            "gqa:mean_y": 0.18,
            "gqa:stddev_x": 0.74,
            "gqa:stddev_xy": 1.08,
            "gqa:stddev_y": 0.78,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:landsat_product_id": "LT05_L1TP_113081_19880330_20170209_01_T1",
            "landsat:landsat_scene_id": "LT51130811988090ASA00",
            "landsat:wrs_path": 113,
            "landsat:wrs_row": 81,
            "odc:dataset_version": "3.1.20200605",
            "odc:file_format": "GeoTIFF",
            "odc:producer": "ga.gov.au",
            "odc:product_family": "ard",
            "odc:region_code": "113081",
            "proj:epsg": 32650,
            "proj:shape": [6981, 7791],
            "proj:transform": [
                30.0,
                0.0,
                233985.0,
                0.0,
                -30.0,
                -3248685.0,
                0.0,
                0.0,
                1.0,
            ],
            "view:sun_azimuth": 55.71404191,
            "view:sun_elevation": 38.53058787,
        },
        "assets": {
            "nbar_nir": {
                "title": "nbar_nir",
                "eo:bands": [{"name": "nbar_nir"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band04.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbar_red": {
                "title": "nbar_red",
                "eo:bands": [{"name": "nbar_red"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band03.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_fmask": {
                "title": "oa_fmask",
                "eo:bands": [{"name": "oa_fmask"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_fmask.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbar_blue": {
                "title": "nbar_blue",
                "eo:bands": [{"name": "nbar_blue"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band01.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_nir": {
                "title": "nbart_nir",
                "eo:bands": [{"name": "nbart_nir"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band04.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_red": {
                "title": "nbart_red",
                "eo:bands": [{"name": "nbart_red"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band03.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbar_green": {
                "title": "nbar_green",
                "eo:bands": [{"name": "nbar_green"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band02.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_blue": {
                "title": "nbart_blue",
                "eo:bands": [{"name": "nbart_blue"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band01.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbar_swir_1": {
                "title": "nbar_swir_1",
                "eo:bands": [{"name": "nbar_swir_1"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band05.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbar_swir_2": {
                "title": "nbar_swir_2",
                "eo:bands": [{"name": "nbar_swir_2"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band07.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_green": {
                "title": "nbart_green",
                "eo:bands": [{"name": "nbart_green"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band02.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_swir_1": {
                "title": "nbart_swir_1",
                "eo:bands": [{"name": "nbart_swir_1"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band05.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "nbart_swir_2": {
                "title": "nbart_swir_2",
                "eo:bands": [{"name": "nbart_swir_2"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band07.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_time_delta": {
                "title": "oa_time_delta",
                "eo:bands": [{"name": "oa_time_delta"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_time-delta.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_solar_zenith": {
                "title": "oa_solar_zenith",
                "eo:bands": [{"name": "oa_solar_zenith"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_solar-zenith.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_exiting_angle": {
                "title": "oa_exiting_angle",
                "eo:bands": [{"name": "oa_exiting_angle"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_exiting-angle.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_solar_azimuth": {
                "title": "oa_solar_azimuth",
                "eo:bands": [{"name": "oa_solar_azimuth"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_solar-azimuth.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_incident_angle": {
                "title": "oa_incident_angle",
                "eo:bands": [{"name": "oa_incident_angle"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_incident-angle.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_relative_slope": {
                "title": "oa_relative_slope",
                "eo:bands": [{"name": "oa_relative_slope"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_relative-slope.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_satellite_view": {
                "title": "oa_satellite_view",
                "eo:bands": [{"name": "oa_satellite_view"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_satellite-view.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_nbar_contiguity": {
                "title": "oa_nbar_contiguity",
                "eo:bands": [{"name": "oa_nbar_contiguity"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_nbar-contiguity.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_nbart_contiguity": {
                "title": "oa_nbart_contiguity",
                "eo:bands": [{"name": "oa_nbart_contiguity"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_nbart-contiguity.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_relative_azimuth": {
                "title": "oa_relative_azimuth",
                "eo:bands": [{"name": "oa_relative_azimuth"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_relative-azimuth.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_azimuthal_exiting": {
                "title": "oa_azimuthal_exiting",
                "eo:bands": [{"name": "oa_azimuthal_exiting"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_azimuthal-exiting.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_satellite_azimuth": {
                "title": "oa_satellite_azimuth",
                "eo:bands": [{"name": "oa_satellite_azimuth"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_satellite-azimuth.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_azimuthal_incident": {
                "title": "oa_azimuthal_incident",
                "eo:bands": [{"name": "oa_azimuthal_incident"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_azimuthal-incident.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "oa_combined_terrain_shadow": {
                "title": "oa_combined_terrain_shadow",
                "eo:bands": [{"name": "oa_combined_terrain_shadow"}],
                "proj:epsg": 32650,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_combined-terrain-shadow.tif",
                "proj:shape": [6981, 7791],
                "proj:transform": [
                    30.0,
                    0.0,
                    233985.0,
                    0.0,
                    -30.0,
                    -3248685.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "checksum:sha1": {
                "type": "text/plain",
                "href": "file://example.com/test_dataset/ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final.sha1",
                "roles": ["metadata"],
            },
            "thumbnail:nbar": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_thumbnail.jpg",
            },
            "thumbnail:nbart": {
                "title": "Thumbnail image",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_thumbnail.jpg",
            },
            "metadata:processor": {
                "type": "text/yaml",
                "roles": ["metadata"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final.proc-info.yaml",
            },
        },
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "href": "http://localhost/stac/collections/ga_ls5t_ard_3/items/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
            },
            {
                "title": "ODC Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": "http://localhost/dataset/5b2f2c50-e618-4bef-ba1f-3d436d9aed14.odc-metadata.yaml",
            },
            {
                "rel": "collection",
                "href": "http://localhost/stac/collections/ga_ls5t_ard_3",
            },
            {
                "title": "ODC Product Overview",
                "rel": "product_overview",
                "type": "text/html",
                "href": "http://localhost/product/ga_ls5t_ard_3",
            },
            {
                "title": "ODC Dataset Overview",
                "rel": "alternative",
                "type": "text/html",
                "href": "http://localhost/dataset/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
            },
            {
                "rel": "root",
                "href": "http://localhost/stac",
            },
        ],
    }
    assert_matching_eo3(response, expected)
