from datetime import datetime
from pathlib import Path
from pprint import pprint
from textwrap import dedent
from typing import Dict
from uuid import UUID

import pytest
from dateutil import tz
from dateutil.tz import tzutc
from flask import Response
from flask.testing import FlaskClient
from geoalchemy2.shape import to_shape
from ruamel import yaml

from cubedash import _utils
from cubedash.summary import _extents, SummaryStore
from cubedash.warmup import find_examples_of_all_public_urls
from datacube.index import Index
from datacube.utils import parse_time
from integration_tests.test_pages_render import assert_all_urls_render
from integration_tests.test_stac import get_item

TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_EO3_DATASET_L1 = (
    TEST_DATA_DIR / "LT05_L1TP_113081_19880330_20170209_01_T1.odc-metadata.yaml"
)
TEST_EO3_DATASET_ARD = (
    TEST_DATA_DIR / "ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final.odc-metadata.yaml"
)


@pytest.fixture(scope="module")
def eo3_index(module_dea_index: Index, dataset_loader):

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

    # We need postgis and some support tables (eg. srid lookup).
    SummaryStore.create(module_dea_index).init()

    return module_dea_index


def test_eo3_extents(eo3_index: Index):
    """
    Do we extract the elements of an EO3 extent properly?

    (ie. not the older grid_spatial definitions)
    """
    [dataset_extent_row] = _extents.get_sample_dataset("ga_ls5t_ard_3", index=eo3_index)
    pprint(dataset_extent_row)

    assert dataset_extent_row["id"] == UUID("5b2f2c50-e618-4bef-ba1f-3d436d9aed14")

    assert dataset_extent_row["center_time"] == datetime(
        1988, 3, 30, 1, 41, 16, 855723, tzinfo=tz.tzutc()
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
        raw_doc = yaml.load(f)

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


def test_eo3_stac_item(eo3_index, client: FlaskClient):
    # Load one stac dataset from the test data.
    response = get_item(
        client,
        "http://localhost/collections/ga_ls5t_ard_3/items/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
    )

    # Our item document can still be improved. This is ensuring changes are deliberate.
    assert response == {
        "stac_version": "1.0.0-beta.2",
        "id": "5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
        "type": "Feature",
        "bbox": [
            114.21535558993,
            -31.2504379233686,
            116.649076384043,
            -29.3490506631636,
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
            "datetime": "1988-03-30T01:41:16.855723+00:00",
            "platform": "landsat-5",
            "instruments": ["tm"],
            "odc:product": "ga_ls5t_ard_3",
            "odc:processing_datetime": "2020-06-05T07:15:26.599544+00:00",
            "cubedash:region_code": "113081",
            "start_datetime": "1988-03-30T01:41:03.171855+00:00",
            "end_datetime": "1988-03-30T01:41:30.539592+00:00",
        },
        "assets": {
            "nbar_nir": {
                "eo:bands": ["nbar_nir"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band04.tif",
            },
            "nbar_red": {
                "eo:bands": ["nbar_red"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band03.tif",
            },
            "oa_fmask": {
                "eo:bands": ["oa_fmask"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_fmask.tif",
            },
            "nbar_blue": {
                "eo:bands": ["nbar_blue"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band01.tif",
            },
            "nbart_nir": {
                "eo:bands": ["nbart_nir"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band04.tif",
            },
            "nbart_red": {
                "eo:bands": ["nbart_red"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band03.tif",
            },
            "nbar_green": {
                "eo:bands": ["nbar_green"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band02.tif",
            },
            "nbart_blue": {
                "eo:bands": ["nbart_blue"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band01.tif",
            },
            "nbar_swir_1": {
                "eo:bands": ["nbar_swir_1"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band05.tif",
            },
            "nbar_swir_2": {
                "eo:bands": ["nbar_swir_2"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbar_3-1-20200605_113081_1988-03-30_final_band07.tif",
            },
            "nbart_green": {
                "eo:bands": ["nbart_green"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band02.tif",
            },
            "nbart_swir_1": {
                "eo:bands": ["nbart_swir_1"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band05.tif",
            },
            "nbart_swir_2": {
                "eo:bands": ["nbart_swir_2"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_nbart_3-1-20200605_113081_1988-03-30_final_band07.tif",
            },
            "oa_time_delta": {
                "eo:bands": ["oa_time_delta"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_time-delta.tif",
            },
            "oa_solar_zenith": {
                "eo:bands": ["oa_solar_zenith"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_solar-zenith.tif",
            },
            "oa_exiting_angle": {
                "eo:bands": ["oa_exiting_angle"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_exiting-angle.tif",
            },
            "oa_solar_azimuth": {
                "eo:bands": ["oa_solar_azimuth"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_solar-azimuth.tif",
            },
            "oa_incident_angle": {
                "eo:bands": ["oa_incident_angle"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_incident-angle.tif",
            },
            "oa_relative_slope": {
                "eo:bands": ["oa_relative_slope"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_relative-slope.tif",
            },
            "oa_satellite_view": {
                "eo:bands": ["oa_satellite_view"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_satellite-view.tif",
            },
            "oa_nbar_contiguity": {
                "eo:bands": ["oa_nbar_contiguity"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_nbar-contiguity.tif",
            },
            "oa_nbart_contiguity": {
                "eo:bands": ["oa_nbart_contiguity"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_nbart-contiguity.tif",
            },
            "oa_relative_azimuth": {
                "eo:bands": ["oa_relative_azimuth"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_relative-azimuth.tif",
            },
            "oa_azimuthal_exiting": {
                "eo:bands": ["oa_azimuthal_exiting"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_azimuthal-exiting.tif",
            },
            "oa_satellite_azimuth": {
                "eo:bands": ["oa_satellite_azimuth"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_satellite-azimuth.tif",
            },
            "oa_azimuthal_incident": {
                "eo:bands": ["oa_azimuthal_incident"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_azimuthal-incident.tif",
            },
            "oa_combined_terrain_shadow": {
                "eo:bands": ["oa_combined_terrain_shadow"],
                "href": "file://example.com/test_dataset/"
                "ga_ls5t_oa_3-1-20200605_113081_1988-03-30_final_combined-terrain-shadow.tif",
            },
            "location": {
                "href": "file://example.com/test_dataset/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
                "odc:secondary_hrefs": [],
            },
        },
        "links": [
            {
                "rel": "self",
                "href": "http://localhost/collections/ga_ls5t_ard_3/items/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
            },
            {"rel": "parent", "href": "http://localhost/collections/ga_ls5t_ard_3"},
            {
                "rel": "alternative",
                "type": "text/html",
                "href": "http://localhost/dataset/5b2f2c50-e618-4bef-ba1f-3d436d9aed14",
            },
        ],
    }
