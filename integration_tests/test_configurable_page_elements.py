from __future__ import annotations

import pytest

from integration_tests.asserts import get_html, get_text_response

TYPE_CHECKING = False
if TYPE_CHECKING:
    from flask.testing import FlaskClient

    from cubedash.summary import SummaryStore

METADATA_TYPES = [
    "metadata/eo3_metadata.yaml",
    "metadata/eo3_landsat_l1.odc-type.yaml",
    "metadata/eo3_landsat_ard.odc-type.yaml",
]
PRODUCTS = [
    "products/ard_ls5.odc-product.yaml",
    "products/ga_ls7e_ard_3.odc-product.yaml",
    "products/ga_ls8c_ard_3.odc-product.yaml",
    "products/l1_ls5.odc-product.yaml",
    "products/l1_ls8_ga.odc-product.yaml",
    "products/usgs_ls7e_level1_1.odc-product.yaml",
]
DATASETS = ["datasets/ga_ls7e_ard_3-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


@pytest.fixture()
def app_configured_client(client: FlaskClient) -> FlaskClient:
    client.application.config.update(
        {
            "CUBEDASH_INSTANCE_TITLE": "Development - ODC",
            "CUBEDASH_SISTER_SITES": (
                ("Production - ODC", "http://prod.odc.example"),
                ("Production - NCI", "http://nci.odc.example"),
            ),
            "CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST": [
                "usgs_ls5t_level1_1",
                "ga_ls8c_level1_3",
                "usgs_ls7e_level1_1",
            ],
            "CUBEDASH_BASEMAP_URL_TEMPLATE": "https://vector.example.com/styles/dummy",
            "CUBEDASH_BASEMAP_ATTRIBUTION": '&copy;<a href="https://example.com/attribution">Vector Basemaps</a>',
        }
    )
    return client


@pytest.fixture()
def app_configured_raster_basemap_client(client: FlaskClient) -> FlaskClient:
    client.application.config.update(
        {
            "CUBEDASH_INSTANCE_TITLE": "Development - ODC",
            "CUBEDASH_SISTER_SITES": (
                ("Production - ODC", "http://prod.odc.example"),
                ("Production - NCI", "http://nci.odc.example"),
            ),
            "CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST": [
                "usgs_ls5t_level1_1",
                "ga_ls8c_level1_3",
                "usgs_ls7e_level1_1",
            ],
            "CUBEDASH_BASEMAP_URL_TEMPLATE": "https://raster.example.com/styles/dummy/{z}/{y}/{x}.png?api_key=test_api_key",
            "CUBEDASH_BASEMAP_ATTRIBUTION": '&copy;<a href="https://example.com/attribution">Raster Basemaps</a>',
        }
    )
    return client


@pytest.fixture()
def total_indexed_products_count(summary_store: SummaryStore) -> int:
    return len(list(summary_store.index.products.get_all()))


def test_instance_title(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/about")

    instance_title = html.css_first(".instance-title").text(strip=True)
    assert instance_title == "Development - ODC"


def test_hide_products_audit_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/audit/storage")
    hidden_product_count = html.css_first("span.hidden-product-count").text(strip=True)
    assert hidden_product_count == "3"

    h2 = html.css_first("h2").text(strip=True)
    indexed_product_count = html.css_first("span.indexed-product-count").text(
        strip=True
    )
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 3) in h2


def test_hide_products_audit_bulk_dataset_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/audit/dataset-counts")
    hidden_product_count = html.css_first("span.hidden-product-count").text(strip=True)
    assert hidden_product_count == "3"

    h2 = html.css_first("h2").text(strip=True)
    indexed_product_count = html.css_first("span.indexed-product-count").text(
        strip=True
    )
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 3) in h2


def test_hide_products_product_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/products")

    h2 = html.css_first("h2").text(strip=True)
    hidden_product_rows = html.css("table.hidden-products tr")
    hidden_product_count = len(hidden_product_rows)
    assert hidden_product_count == 3
    assert str(total_indexed_products_count - 3) in h2

    listed_product_count = html.css("tr.collapse-when-small")
    assert len(listed_product_count) == (total_indexed_products_count - 3)


def test_hide_products_menu_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/about")

    hide_products = html.css("#products-menu li a.configured-hide-product")
    assert len(hide_products) == 3

    products_hide_show_switch = html.css_first("a#show-hidden-product")
    assert products_hide_show_switch is not None

    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3")
    products = html.css(".product-selection-header a.option-menu-link")
    assert total_indexed_products_count - len(products) == 3


def test_sister_sites(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/about")

    sister_instances = html.css("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        href = sister_instance.css_first("a.sister-link").attributes["href"]
        assert href is not None
        assert "/about" in href


def test_sister_sites_request_path(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3")

    sister_instances = html.css("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        href = sister_instance.css_first("a.sister-link").attributes["href"]
        assert href is not None
        assert "/products/ga_ls5t_ard_3" in href

    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3/datasets")

    sister_instances = html.css("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        href = sister_instance.css_first("a.sister-link").attributes["href"]
        assert href is not None
        assert "/products/ga_ls5t_ard_3/datasets" in href


def test_vector_basemap(app_configured_client: FlaskClient) -> None:
    for url in (
        "/products/ga_ls7e_ard_3",
        "/products/ga_ls7e_ard_3/datasets/50014f19-5546-4853-be8d-0185a798c083",
        "/product/ga_ls7e_ard_3/regions/093074",
    ):
        txt, _ = get_text_response(app_configured_client, url)

        # Check defaults not displayed
        assert "tiles.openfreemap.org/styles/positron" not in txt
        assert "https://vector.example.com/styles/dummy" in txt
        assert "https://example.com/attribution" in txt
        assert "Vector Basemaps" in txt


def test_raster_basemap(app_configured_raster_basemap_client: FlaskClient) -> None:
    for url in (
        "/products/ga_ls7e_ard_3",
        "/products/ga_ls7e_ard_3/datasets/50014f19-5546-4853-be8d-0185a798c083",
        "/product/ga_ls7e_ard_3/regions/093074",
    ):
        txt, _ = get_text_response(app_configured_raster_basemap_client, url)

        # Check defaults not displayed
        assert "tiles.openfreemap.org/styles/positron" not in txt
        assert "https://raster.example.com/styles/dummy/" in txt
        assert "https://example.com/attribution" in txt
        assert "Raster Basemaps" in txt
