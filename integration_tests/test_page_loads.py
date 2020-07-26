"""
Tests that load pages and check the contained text.
"""
import json

import pytest
from click.testing import Result
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient
from requests_html import HTML

import cubedash
from cubedash import _model, _monitoring
from cubedash.summary import SummaryStore, _extents, show
from datacube.index import Index
from integration_tests.asserts import (
    check_area,
    check_dataset_count,
    check_last_processed,
    get_geojson,
    get_html,
)

DEFAULT_TZ = tz.gettz("Australia/Darwin")


@pytest.fixture(scope="module", autouse=True)
def auto_populate_index(populated_index):
    """
    Auto-populate the index for all tests in this file.
    """
    return populated_index


@pytest.fixture()
def sentry_client(client: FlaskClient) -> FlaskClient:
    cubedash.app.config["SENTRY_CONFIG"] = {
        "dsn": "___DSN___",
        "include_paths": ["cubedash"],
    }
    return client


def test_sentry(sentry_client: FlaskClient):
    rv: Response = sentry_client.get("/", follow_redirects=False)
    # Redirect to a default.
    assert rv.location.endswith("/ls7_nbar_scene")


def test_default_redirect(client: FlaskClient):
    rv: Response = client.get("/", follow_redirects=False)
    # Redirect to a default.
    assert rv.location.endswith("/ls7_nbar_scene")


def test_get_overview(client: FlaskClient):
    html = get_html(client, "/wofs_albers")
    check_dataset_count(html, 11)
    check_last_processed(html, "2018-05-20T11:25:35")
    assert "wofs_albers whole collection" in _h1_text(html)
    check_area("61,...km2", html)

    html = get_html(client, "/wofs_albers/2017")

    check_dataset_count(html, 11)
    check_last_processed(html, "2018-05-20T11:25:35")
    assert "wofs_albers across 2017" in _h1_text(html)

    html = get_html(client, "/wofs_albers/2017/04")
    check_dataset_count(html, 4)
    check_last_processed(html, "2018-05-20T09:36:57")
    assert "wofs_albers across April 2017" in _h1_text(html)
    check_area("30,...km2", html)


def test_invalid_footprint_wofs_summary_load(client: FlaskClient):
    # This all-time overview has a valid footprint that becomes invalid
    # when reprojected to wrs84 by shapely.
    from .data_wofs_summary import wofs_time_summary

    _model.STORE._do_put("wofs_summary", None, None, None, wofs_time_summary)
    html = get_html(client, "/wofs_summary")
    check_dataset_count(html, 1244)

    d = get_geojson(client, "/api/regions/wofs_summary")
    assert len(d["features"]) == 1244


def test_all_products_are_shown(client: FlaskClient):
    """
    After all the complicated grouping logic, there should still be one header link for each product.
    """
    html = get_html(client, "/ls7_nbar_scene")

    # We use a sorted array instead of a Set to detect duplicates too.
    found_product_names = sorted(
        [
            a.text.strip()
            for a in html.find(".product-selection-header .option-menu-link")
        ]
    )
    indexed_product_names = sorted(p.name for p in _model.STORE.all_dataset_types())
    assert (
        found_product_names == indexed_product_names
    ), "Product shown in menu don't match the indexed products"


def test_get_overview_product_links(client: FlaskClient):
    """
    Are the source and derived product lists being displayed?
    """
    html = get_html(client, "/ls7_nbar_scene/2017")

    product_links = html.find(".source-product a")
    assert [p.text for p in product_links] == ["ls7_level1_scene"]
    assert [p.attrs["href"] for p in product_links] == ["/ls7_level1_scene/2017"]

    product_links = html.find(".derived-product a")
    assert [p.text for p in product_links] == ["ls7_pq_legacy_scene"]
    assert [p.attrs["href"] for p in product_links] == ["/ls7_pq_legacy_scene/2017"]


def test_get_day_overviews(client: FlaskClient):
    # Individual days are computed on-the-fly rather than from summaries, so can
    # have their own issues.

    # With a dataset
    html = get_html(client, "/ls7_nbar_scene/2017/4/20")
    check_dataset_count(html, 1)
    assert "ls7_nbar_scene on 20th April 2017" in _h1_text(html)

    # Empty day
    html = get_html(client, "/ls7_nbar_scene/2017/4/22")
    check_dataset_count(html, 0)


def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/pq_count_summary")
    check_dataset_count(html, 20)


def test_uninitialised_overview(
    unpopulated_client: FlaskClient, summary_store: SummaryStore
):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    # Then load an unpopulated product.
    summary_store.get_or_update("ls7_nbar_albers")
    html = get_html(unpopulated_client, "/ls7_nbar_scene/2017")
    assert html.find(".coverage-region-count", first=True).text == "0 unique scenes"


def test_uninitialised_search_page(
    empty_client: FlaskClient, summary_store: SummaryStore
):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    summary_store.refresh_product(
        summary_store.index.products.get_by_name("ls7_nbar_albers")
    )
    summary_store.get_or_update("ls7_nbar_albers")

    # Then load a completely uninitialised product.
    html = get_html(empty_client, "/datasets/ls7_nbar_scene")
    search_results = html.find(".search-result a")
    assert len(search_results) == 4


def test_view_dataset(client: FlaskClient):
    # ls7_level1_scene dataset
    html = get_html(client, "/dataset/57848615-2421-4d25-bfef-73f57de0574d")

    # Label of dataset is header
    assert "LS7_ETM_OTH_P51_GALPGS01-002_105_074_20170501" in _h1_text(html)

    # wofs_albers dataset (has no label or location)
    rv: Response = client.get("/dataset/20c024b5-6623-4b06-b00c-6b5789f81eeb")
    assert b"-20.502 to -19.6" in rv.data
    assert b"132.0 to 132.924" in rv.data

    # No dataset found: should return 404, not a server error.
    rv: Response = client.get("/dataset/de071517-af92-4dd7-bf91-12b4e7c9a435")
    assert rv.status_code == 404
    assert b"No dataset found" in rv.data


def _h1_text(html):
    return _text(html, "h1")


def _text(html, tag):
    return html.find(tag, first=True).text


def test_view_product(client: FlaskClient):
    rv: Response = client.get("/product/ls7_nbar_scene")
    assert b"Landsat 7 NBAR 25 metre" in rv.data


def test_view_metadata_type(client: FlaskClient):
    # Does it load without error?
    html: HTML = get_html(client, "/metadata-type/eo")
    assert "eo Metadata Type" in _text(html, "h2")

    # Does the page list products using the type?
    products_using_it = [t.text for t in html.find(".type-usage-item")]
    assert "ls8_nbar_albers" in products_using_it


def test_about_page(client: FlaskClient):
    rv: Response = client.get("/about")
    assert b"wofs_albers" in rv.data
    assert b"11 total datasets" in rv.data


@pytest.mark.skip(reason="TODO: fix out-of-date range return value")
def test_out_of_date_range(client: FlaskClient):
    """
    We have generated summaries for this product, but the date is out of the product's date range.
    """
    html = get_html(client, "/wofs_albers/2010")

    # The common error here is to say "No data: not yet generated" rather than "0 datasets"
    assert check_dataset_count(html, 0)
    assert b"Historic Flood Mapping Water Observations from Space" in html.text


def test_loading_high_low_tide_comp(client: FlaskClient):
    html = get_html(client, "/high_tide_comp_20p/2008")

    assert (
        html.search("High Tide 20 percentage composites for entire coastline")
        is not None
    )

    check_dataset_count(html, 306)
    # Footprint is not exact due to shapely.simplify()
    check_area("2,984,...km2", html)

    assert (
        html.find(".last-processed time", first=True).attrs["datetime"]
        == "2017-06-08T20:58:07.014314+00:00"
    )


def test_api_returns_high_tide_comp_datasets(client: FlaskClient):
    """
    These are slightly fun to handle as they are a small number with a huge time range.
    """
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p")
    assert (
        len(geojson["features"]) == 306
    ), "Not all high tide datasets returned as geojson"

    # Search and time summary is only based on center time.
    # These searches are within the dataset time range, but not the center_time.
    # Dataset range: '2000-01-01T00:00:00' to '2016-10-31T00:00:00'
    # year
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2008")
    assert (
        len(geojson["features"]) == 306
    ), "Expected high tide datasets within whole dataset range"
    # month
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2008/6")
    assert (
        len(geojson["features"]) == 306
    ), "Expected high tide datasets within whole dataset range"
    # day
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2008/6/1")
    assert (
        len(geojson["features"]) == 306
    ), "Expected high tide datasets within whole dataset range"

    # Out of the test dataset time range. No results.

    # Completely outside of range
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2018")
    assert (
        len(geojson["features"]) == 0
    ), "Expected no high tide datasets in in this year"
    # One day before/after (is time zone handling correct?)
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2008/6/2")
    assert len(geojson["features"]) == 0, "Expected no result one-day-after center time"
    geojson = get_geojson(client, "/api/datasets/high_tide_comp_20p/2008/5/31")
    assert len(geojson["features"]) == 0, "Expected no result one-day-after center time"


def test_api_returns_scenes_as_geojson(client: FlaskClient):
    """
    L1 scenes have no footprint, falls back to bounds. Have weird CRSes too.
    """
    geojson = get_geojson(client, "/api/datasets/ls8_level1_scene")
    assert len(geojson["features"]) == 7, "Unexpected scene polygon count"


def test_api_returns_tiles_as_geojson(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = get_geojson(client, "/api/datasets/ls7_nbart_albers")
    assert len(geojson["features"]) == 4, "Unepected albers polygon count"


def test_api_returns_high_tide_comp_regions(client: FlaskClient):
    """
    High tide doesn't have anything we can use as regions.

    It should be empty (no regions supported) rather than throw an exception.
    """

    rv: Response = client.get("/api/regions/high_tide_comp_20p")
    assert (
        rv.status_code == 404
    ), "High tide comp does not support regions: it should return not-exist code."


def test_api_returns_scene_regions(client: FlaskClient):
    """
    L1 scenes have no footprint, falls back to bounds. Have weird CRSes too.
    """
    geojson = get_geojson(client, "/api/regions/ls8_level1_scene")
    assert len(geojson["features"]) == 7, "Unexpected scene region count"


def test_region_page(client: FlaskClient):
    """
    Load a list of scenes for a given region.
    """
    html = get_html(client, "/region/ls7_nbar_scene/96_82")
    search_results = html.find(".search-result a")
    assert len(search_results) == 1
    result = search_results[0]
    assert result.text == "LS7_ETM_NBAR_P54_GANBAR01-002_096_082_20170502"

    # If "I'm feeling lucky", and only one result, redirect straight to it.
    response: Response = client.get("/region/ls7_nbar_scene/96_82?feelinglucky")
    assert response.status_code == 302
    assert response.location.endswith("/dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c")


def test_search_page(client: FlaskClient):
    html = get_html(client, "/datasets/ls7_nbar_scene")
    search_results = html.find(".search-result a")
    assert len(search_results) == 4

    html = get_html(client, "/datasets/ls7_nbar_scene/2017/05")
    search_results = html.find(".search-result a")
    assert len(search_results) == 3


def test_search_time_completion(client: FlaskClient):
    # They only specified a begin time, so the end time should be filled in with the product extent.
    html = get_html(client, "/datasets/ls7_nbar_scene?time-begin=1999-05-28")
    assert html.find("#search-time-before", first=True).attrs["value"] == "1999-05-28"
    # One day after the product extent end (range is exclusive)
    assert html.find("#search-time-after", first=True).attrs["value"] == "2017-05-04"
    search_results = html.find(".search-result a")
    assert len(search_results) == 4


def test_api_returns_tiles_regions(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = get_geojson(client, "/api/regions/ls7_nbart_albers")
    assert len(geojson["features"]) == 4, "Unexpected albers region count"


def test_api_returns_limited_tile_regions(client: FlaskClient):
    """
    Covers most of the 'normal' products: they have a footprint, bounds and a simple crs epsg code.
    """
    geojson = get_geojson(client, "/api/regions/wofs_albers/2017/04")
    assert len(geojson["features"]) == 4, "Unexpected wofs albers region month count"
    geojson = get_geojson(client, "/api/regions/wofs_albers/2017/04/20")
    print(json.dumps(geojson, indent=4))
    assert len(geojson["features"]) == 1, "Unexpected wofs albers region day count"
    geojson = get_geojson(client, "/api/regions/wofs_albers/2017/04/6")
    assert len(geojson["features"]) == 0, "Unexpected wofs albers region count"


def test_undisplayable_product(client: FlaskClient):
    """
    Telemetry products have no footprint available at all.
    """
    html = get_html(client, "/ls7_satellite_telemetry_data")
    check_dataset_count(html, 4)
    assert "36.6GiB" in html.find(".coverage-filesize", first=True).text
    assert "(None displayable)" in html.text
    assert "No CRSes defined" in html.text


def test_no_data_pages(client: FlaskClient):
    """
    Fetch products that exist but have no summaries generated.

    (these should load with "empty" messages: not throw exceptions)
    """
    html = get_html(client, "/ls8_nbar_albers/2017")
    assert "No data: not yet generated" in html.text
    assert "Unknown number of datasets" in html.text

    html = get_html(client, "/ls8_nbar_albers/2017/5")
    assert "No data: not yet generated" in html.text
    assert "Unknown number of datasets" in html.text

    # Days are generated on demand: it should query and see that there are no datasets.
    html = get_html(client, "/ls8_nbar_albers/2017/5/2")
    check_dataset_count(html, 0)


def test_missing_dataset(client: FlaskClient):
    rv: Response = client.get("/datasets/f22a33f4-42f2-4aa5-9b20-cee4ca4a875c")
    assert rv.status_code == 404


def test_invalid_product(client: FlaskClient):
    rv: Response = client.get("/fake_test_product/2017")
    assert rv.status_code == 404


def test_show_summary_cli(clirunner, client: FlaskClient):
    # ls7_nbar_scene / 2017 / 05
    res: Result = clirunner(show.cli, ["ls7_nbar_scene", "2017", "5"])
    print(res.output)
    assert "Landsat WRS scene-based product" in res.output
    assert "3 ls7_nbar_scene datasets for 2017 5" in res.output
    assert "727.4MiB" in res.output
    assert (
        "96  97  98  99 100 101 102 103 104 105" in res.output
    ), "No list of paths displayed"


def test_extent_debugging_method(module_dea_index: Index, client: FlaskClient):
    [cols] = _extents.get_sample_dataset("ls7_nbar_scene", index=module_dea_index)
    assert cols["id"] is not None
    assert cols["dataset_type_ref"] is not None
    assert cols["center_time"] is not None
    assert cols["footprint"] is not None

    # Can it be serialised without type errors? (for printing)
    output_json = _extents._as_json(cols)
    assert str(cols["id"]) in output_json

    [cols] = _extents.get_mapped_crses("ls7_nbar_scene", index=module_dea_index)
    assert cols["product"] == "ls7_nbar_scene"
    assert cols["crs"] in (28349, 28350, 28351, 28352, 28353, 28354, 28355, 28356)


@pytest.mark.skip(
    reason="TODO: fix issue https://github.com/opendatacube/datacube-explorer/issues/92"
)
def test_with_timings(client: FlaskClient):
    _monitoring.init_app_monitoring()
    # ls7_level1_scene dataset
    rv: Response = client.get("/dataset/57848615-2421-4d25-bfef-73f57de0574d")
    assert "Server-Timing" in rv.headers

    count_header = [
        f
        for f in rv.headers["Server-Timing"].split(",")
        if f.startswith("odcquerycount_")
    ]
    assert (
        count_header
    ), f"No query count server timing header found in {rv.headers['Server-Timing']}"

    # Example header:
    # app;dur=1034.12,odcquery;dur=103.03;desc="ODC query time",odcquerycount_6;desc="6 ODC queries"
    _, val = count_header[0].split(";")[0].split("_")
    assert int(val) > 0, "At least one query was run, presumably?"


def test_plain_product_list(client: FlaskClient):
    rv: Response = client.get("/products.txt")
    assert "ls7_nbar_scene\n" in rv.data.decode("utf-8")
