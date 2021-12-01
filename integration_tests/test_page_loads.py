"""
Tests that load pages and check the contained text.
"""
import json
from datetime import datetime
from io import StringIO
from textwrap import indent

import pytest
from click.testing import Result
from datacube.index import Index
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient
from requests_html import HTML, Element
from ruamel.yaml import YAML, YAMLError

import cubedash
from cubedash import _model, _monitoring
from cubedash.summary import SummaryStore, _extents, show
from integration_tests.asserts import (
    check_area,
    check_dataset_count,
    check_last_processed,
    get_geojson,
    get_html,
    get_text_response,
)

DEFAULT_TZ = tz.gettz("Australia/Darwin")


@pytest.fixture(scope="module", autouse=True)
def auto_populate_index(populated_index: Index):
    """
    Auto-populate the index for all tests in this file.
    """
    populated_product_counts = {
        p.name: count for p, count in populated_index.datasets.count_by_product()
    }
    assert populated_product_counts == {
        "dsm1sv10": 1,
        "high_tide_comp_20p": 306,
        "ls7_level1_scene": 4,
        "ls7_nbar_scene": 4,
        "ls7_nbart_albers": 4,
        "ls7_nbart_scene": 4,
        "ls7_pq_legacy_scene": 4,
        "ls7_satellite_telemetry_data": 4,
        "ls8_level1_scene": 7,
        "ls8_nbar_scene": 7,
        "ls8_nbart_albers": 7,
        "ls8_nbart_scene": 7,
        "ls8_pq_legacy_scene": 7,
        "ls8_satellite_telemetry_data": 7,
        "pq_count_summary": 20,
        "wofs_albers": 11,
    }
    return populated_index


@pytest.fixture()
def sentry_client(client: FlaskClient) -> FlaskClient:
    cubedash.app.config["SENTRY_CONFIG"] = {
        "dsn": "https://githash@number.sentry.opendatacube.org/123456",
        "include_paths": ["cubedash"],
    }
    return client


def _script(html: HTML):
    return html.find("script")


def test_sentry(sentry_client: FlaskClient):
    """Ensure Sentry Client gets initialized correctly

    Args:
        sentry_client (FlaskClient): Client for Flask app with Sentry enabled
    """
    html: HTML = get_html(sentry_client, "/ls7_nbar_scene")
    # Ensure rendered page has a SENTRY link
    assert "raven.min.js" in str(_script(html))


def test_prometheus(sentry_client: FlaskClient):
    """
    Ensure Prometheus metrics endpoint exists
    """
    resp = sentry_client.get("/metrics")
    assert b"flask_exporter_info" in resp.data


def test_default_redirect(client: FlaskClient):
    rv: Response = client.get("/", follow_redirects=False)
    # The products page is the default.
    assert rv.location.endswith("/products")


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
    # when reprojected to wgs84 by shapely.
    from .data_wofs_summary import wofs_time_summary

    _model.STORE._put(wofs_time_summary)
    html = get_html(client, "/wofs_summary")
    check_dataset_count(html, 1244)


def test_all_products_are_shown(client: FlaskClient):
    """
    After all the complicated grouping logic, there should still be one header link for each product.
    """
    html = get_html(client, "/ls7_nbar_scene")

    # We use a sorted array instead of a Set to detect duplicates too.
    found_product_names = sorted(
        a.text.strip() for a in html.find(".product-selection-header .option-menu-link")
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
    assert [p.attrs["href"] for p in product_links] == [
        "/products/ls7_level1_scene/2017"
    ]

    product_links = html.find(".derived-product a")
    assert [p.text for p in product_links] == ["ls7_pq_legacy_scene"]
    assert [p.attrs["href"] for p in product_links] == [
        "/products/ls7_pq_legacy_scene/2017"
    ]


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
    summary_store.refresh("ls7_nbar_albers")

    html = get_html(unpopulated_client, "/ls7_nbar_scene/2017")

    # The page should load without error, but will display 'unknown' fields
    assert html.find("h2", first=True).text == "ls7_nbar_scene: Landsat 7 NBAR 25 metre"
    assert "Unknown number of datasets" in html.text
    assert "No data: not yet summarised" in html.text


def test_uninitialised_product(empty_client: FlaskClient, summary_store: SummaryStore):
    """
    An unsummarised product should still be viewable on the product page.

    (but should be described as not summarised)
    """
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    # Then load an unpopulated product.
    summary_store.refresh("ls7_nbar_albers")

    html = get_html(empty_client, "/products/ls7_nbar_scene")

    # The page should load without error, but will mention its lack of information
    assert "ls7_nbar_scene" in html.find("h2", first=True).text
    assert "not yet summarised" in one_element(html, "#content").text

    # ... and a product that we populated does not have the message:
    html = get_html(empty_client, "/products/ls7_nbar_albers")
    assert "not yet summarised" not in one_element(html, "#content").text


def test_empty_product_overview(client: FlaskClient):
    """
    A page is still displayable without error when it has no datasets.
    """
    html = get_html(client, "/ls5_nbar_scene")
    assert_is_text(html, ".dataset-count", "0 datasets")

    assert_is_text(html, ".query-param.key-platform .value", "LANDSAT_5")
    assert_is_text(html, ".query-param.key-instrument .value", "TM")
    assert_is_text(html, ".query-param.key-product_type .value", "nbar")


def test_empty_product_page(client: FlaskClient):
    """
    A product page is displayable when summarised, but with 0 datasets.
    """
    html = get_html(client, "/products/ls5_nbar_scene")
    assert "0 datasets" in one_element(html, ".dataset-count").text

    # ... yet a normal product doesn't show the message:
    html = get_html(client, "/products/ls7_nbar_scene")
    assert "0 datasets" not in one_element(html, ".dataset-count").text
    assert "4 datasets" in one_element(html, ".dataset-count").text


def one_element(html: HTML, selector: str) -> Element:
    """
    Expect one element on the page to match the given selector, return it.
    """
    __tracebackhide__ = True

    def err(msg: str):
        __tracebackhide__ = True
        raw_text = html.raw_html.decode("utf-8")[600:]
        print(f"Received error on page: {indent(raw_text, ' ' * 4)}")
        raise AssertionError(msg)

    els = html.find(selector)
    if not els:
        err(f"{selector!r} is not in the result.")

    if len(els) > 1:
        err(f"Multiple elements on page match the selector {selector!r}")

    return els[0]


def assert_is_text(html: HTML, selector, text: str):
    __tracebackhide__ = True
    el = one_element(html, selector)
    assert el.text == text


def test_uninitialised_search_page(
    empty_client: FlaskClient, summary_store: SummaryStore
):
    # Populate one product, so they don't get the usage error message ("run cubedash generate")
    summary_store.refresh("ls7_nbar_albers")

    # Then load a completely uninitialised product.
    html = get_html(empty_client, "/datasets/ls7_nbar_scene")
    search_results = html.find(".search-result a")
    assert len(search_results) == 4


def test_view_dataset(client: FlaskClient):
    # ls7_level1_scene dataset
    html = get_html(client, "/dataset/57848615-2421-4d25-bfef-73f57de0574d")

    # Label of dataset is header
    assert (
        "LS7_ETM_OTH_P51_GALPGS01-002_105_074_20170501"
        in html.find("h2", first=True).text
    )

    # wofs_albers dataset (has no label or location)
    rv: HTML = get_html(client, "/dataset/20c024b5-6623-4b06-b00c-6b5789f81eeb")
    assert "-20.502 to -19.6" in rv.text
    assert "132.0 to 132.924" in rv.text

    # No dataset found: should return 404, not a server error.
    rv: Response = client.get(
        "/dataset/de071517-af92-4dd7-bf91-12b4e7c9a435", follow_redirects=True
    )

    assert rv.status_code == 404
    assert b"No dataset found" in rv.data, rv.data.decode("utf-8")


def _h1_text(html):
    return one_element(html, "h1").text


def test_view_product(client: FlaskClient):
    rv: HTML = get_html(client, "/product/ls7_nbar_scene")
    assert "Landsat 7 NBAR 25 metre" in rv.text


def test_view_metadata_type(client: FlaskClient, populated_index: Index):
    # Does it load without error?
    html: HTML = get_html(client, "/metadata-type/eo")
    assert html.find("h2", first=True).text == "eo"

    how_many_are_eo = len(
        [p for p in populated_index.products.get_all() if p.metadata_type.name == "eo"]
    )
    assert (
        html.find(".header-follow", first=True).text
        == f"metadata type of {how_many_are_eo} products"
    )

    # Does the page list products using the type?
    products_using_it = [t.text for t in html.find(".type-usage-item")]
    assert "ls8_nbar_albers" in products_using_it


def test_storage_page(client: FlaskClient, populated_index: Index):
    html: HTML = get_html(client, "/audit/storage")

    assert html.find(".product-name", containing="wofs_albers")

    product_count = len(list(populated_index.products.get_all()))
    assert f"{product_count} products" in html.text
    assert len(html.find(".data-table tbody tr")) == product_count


@pytest.mark.skip(reason="TODO: fix out-of-date range return value")
def test_out_of_date_range(client: FlaskClient):
    """
    We have generated summaries for this product, but the date is out of the product's date range.
    """
    html = get_html(client, "/wofs_albers/2010")

    # The common error here is to say "No data: not yet summarised" rather than "0 datasets"
    assert check_dataset_count(html, 0)
    assert "Historic Flood Mapping Water Observations from Space" in html.text


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
        one_element(html, ".last-processed time").attrs["datetime"]
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
    assert_redirects_to(
        client,
        "/product/ls7_nbar_scene/regions/96_82?feelinglucky=",
        "/dataset/0c5b625e-5432-4911-9f7d-f6b894e27f3c",
    )


def test_legacy_region_redirect(client: FlaskClient):

    # Legacy redirect works, and maintains "feeling lucky"
    assert_redirects_to(
        client,
        "/region/ls7_nbar_scene/96_82?feelinglucky",
        "/product/ls7_nbar_scene/regions/96_82?feelinglucky=",
    )


def assert_redirects_to(client: FlaskClient, url: str, redirects_to_url: str):
    __tracebackhide__ = True
    response: Response = client.get(url, follow_redirects=False)
    assert response.status_code == 302
    assert response.location.endswith(redirects_to_url), (
        f"Expected redirect to end with:\n"
        f"    {redirects_to_url!r}\n"
        f"but was redirected to:\n"
        f"    {response.location!r}"
    )


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
    assert one_element(html, "#search-time-before").attrs["value"] == "1999-05-28"
    # One day after the product extent end (range is exclusive)
    assert one_element(html, "#search-time-after").attrs["value"] == "2017-05-04"
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
    assert "36.6GiB" in one_element(html, ".coverage-filesize").text
    assert "(None displayable)" in html.text
    assert "No CRSes defined" in html.text


def test_no_data_pages(client: FlaskClient):
    """
    Fetch products that exist but have no summaries generated.

    (these should load with "empty" messages: not throw exceptions)
    """
    html = get_html(client, "/ls8_nbar_albers/2017")
    assert "No data: not yet summarised" in html.text
    assert "Unknown number of datasets" in html.text

    html = get_html(client, "/ls8_nbar_albers/2017/5")
    assert "No data: not yet summarised" in html.text
    assert "Unknown number of datasets" in html.text

    # Days are generated on demand: it should query and see that there are no datasets.
    html = get_html(client, "/ls8_nbar_albers/2017/5/2")
    check_dataset_count(html, 0)


def test_general_dataset_redirect(client: FlaskClient):
    """
    When someone queries a dataset UUID, they should be redirected
    to the real URL for the collection.
    """
    rv: Response = client.get(
        "/dataset/57848615-2421-4d25-bfef-73f57de0574d", follow_redirects=False
    )
    # It should be a redirect
    assert rv.status_code == 302
    assert (
        rv.location
        == "http://localhost/products/ls7_level1_scene/datasets/57848615-2421-4d25-bfef-73f57de0574d"
    )


def test_missing_dataset(client: FlaskClient):
    rv: Response = client.get(
        "/products/f22a33f4-42f2-4aa5-9b20-cee4ca4a875c/datasets",
        follow_redirects=False,
    )
    assert rv.status_code == 404

    # But a real dataset definitely works:
    rv: Response = client.get(
        "/products/ls7_level1_scene/datasets/57848615-2421-4d25-bfef-73f57de0574d",
        follow_redirects=False,
    )
    assert rv.status_code == 200


def test_invalid_product_returns_not_found(client: FlaskClient):
    """
    An invalid product should be "not found". No server errors.
    """
    rv: Response = client.get(
        "/products/fake_test_product/2017", follow_redirects=False
    )
    assert rv.status_code == 404


def test_show_summary_cli(clirunner, client: FlaskClient):
    """
    You should be able to view a product with cubedash-view command-line program.
    """
    # ls7_nbar_scene, 2017, May
    res: Result = clirunner(show.cli, ["ls7_nbar_scene", "2017", "5"])
    print(res.output)

    # Expect it to show the dates in local timezone.
    expected_from = datetime(2017, 4, 20, 0, 3, 26, tzinfo=tz.tzutc()).astimezone()
    expected_to = datetime(2017, 5, 3, 1, 6, 41, 500000, tzinfo=tz.tzutc()).astimezone()

    expected_header = "\n".join(
        (
            "ls7_nbar_scene",
            "",
            "3  datasets",
            f"from {expected_from.isoformat()} ",
            f"  to {expected_to.isoformat()} ",
        )
    )
    assert res.output.startswith(expected_header)
    expected_metadata = "\n".join(
        (
            "Metadata",
            "\tgsi: ASA",
            "\torbit: None",
            "\tformat: GeoTIFF",
            "\tplatform: LANDSAT_7",
            "\tinstrument: ETM",
            "\tproduct_type: nbar",
        )
    )
    assert expected_metadata in res.output
    expected_period = "\n".join(
        (
            "Period: 2017 5 all-days",
            "\tStorage size: 727.4MiB",
            "\t3 datasets",
            "",
        )
    )
    assert expected_period in res.output


def test_show_summary_cli_out_of_bounds(clirunner, client: FlaskClient):
    """
    Can you view a date that doesn't exist?
    """
    # A period that's out of bounds.
    res: Result = clirunner(
        show.cli, ["ls7_nbar_scene", "2030", "5"], expect_success=False
    )
    assert "No summary for chosen period." in res.output


def test_show_summary_cli_missing_product(clirunner, client: FlaskClient):
    """
    A missing product should return a nice error message from cubedash-view.

    (and error return code)
    """
    res: Result = clirunner(show.cli, ["does_not_exist"], expect_success=False)
    output: str = res.output
    assert output.strip().startswith("Unknown product 'does_not_exist'")
    assert res.exit_code != 0


def test_show_summary_cli_unsummarised_product(clirunner, empty_client: FlaskClient):
    """
    An unsummarised product should return a nice error message from cubedash-view.

    (and error return code)
    """
    res: Result = clirunner(show.cli, ["ls7_nbar_scene"], expect_success=False)
    out = res.output.strip()
    assert out.startswith("No info: product 'ls7_nbar_scene' has not been summarised")
    assert res.exit_code != 0


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
    text, rv = get_text_response(client, "/products.txt")
    assert "ls7_nbar_scene\n" in text


def test_raw_documents(client: FlaskClient):
    """
    Check that raw-documents load without error,
    and have embedded hints on where they came from (source-url)
    """

    def check_doc_start_has_hint(hint: str, url: str):
        __tracebackhide__ = True
        doc, rv = get_text_response(client, url)
        doc_opening = doc[:128]
        expect_pattern = f"# {hint}\n# url: http://localhost{url}\n"
        assert expect_pattern in doc_opening, (
            f"No hint or source-url in yaml response.\n"
            f"Expected {expect_pattern!r}\n"
            f"Got      {doc_opening!r}"
        )

        try:
            YAML(typ="safe", pure=True).load(StringIO(doc))
        except YAMLError as e:
            raise AssertionError(f"Expected valid YAML document for url {url!r}") from e

    # Product
    check_doc_start_has_hint("Product", "/products/ls8_nbar_albers.odc-product.yaml")

    # Metadata type
    check_doc_start_has_hint("Metadata Type", "/metadata-types/eo3.odc-type.yaml")

    # A legacy EO1 dataset
    check_doc_start_has_hint(
        "EO1 Dataset",
        "/dataset/57848615-2421-4d25-bfef-73f57de0574d.odc-metadata.yaml",
    )


def test_all_give_404s(client: FlaskClient):
    """
    We should get 404 messages, not exceptions, for missing things.
    """

    def expect_404(url: str, message_contains: str = None):
        __tracebackhide__ = True
        response = get_text_response(client, url, expect_status_code=404)
        if message_contains and message_contains not in response:
            raise AssertionError(
                f"Expected {message_contains!r} in response {response!r}"
            )

    name = "does_not_exist"
    time = datetime.utcnow()
    region_code = "not_a_region"
    dataset_id = "37296b9a-e6ec-4bfd-ab80-cc32902429d1"

    expect_404(f"/metadata-types/{name}")
    expect_404(f"/metadata-types/{name}.odc-type.yaml")

    expect_404(f"/datasets/{name}")
    expect_404(f"/products/{name}")
    expect_404(f"/products/{name}.odc-product.yaml")

    expect_404(f"/products/{name}/extents/{time:%Y}")
    expect_404(f"/products/{name}/extents/{time:%Y/%m}")
    expect_404(f"/products/{name}/extents/{time:%Y/%m/%d}")

    expect_404(f"/products/{name}/datasets/{time:%Y}")
    expect_404(f"/products/{name}/datasets/{time:%Y/%m}")
    expect_404(f"/products/{name}/datasets/{time:%Y/%m/%d}")

    expect_404(f"/region/{name}/{region_code}")
    expect_404(f"/region/{name}/{region_code}/{time:%Y/%m/%d}")

    expect_404(f"/dataset/{dataset_id}")
    expect_404(f"/dataset/{dataset_id}.odc-metadata.yaml")
