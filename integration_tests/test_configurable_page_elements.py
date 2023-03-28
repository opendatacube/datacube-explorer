import pytest
from flask.testing import FlaskClient

import cubedash
from cubedash.summary import SummaryStore
from integration_tests.asserts import get_html

METADATA_TYPES = ["metadata/eo_metadata.yaml", "metadata/landsat_l1_scene.yaml"]
PRODUCTS = [
    "products/ls5_fc_albers.odc-product.yaml",
    "products/ls5_scenes.odc-product.yaml",
    "products/ls7_scenes.odc-product.yaml",
    "products/ls8_scenes.odc-product.yaml",
    "products/dsm1sv10.odc-product.yaml",
]
DATASETS = ["datasets/ls5_fc_albers-sample.yaml"]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


@pytest.fixture()
def app_configured_client(client: FlaskClient):
    cubedash.app.config["CUBEDASH_INSTANCE_TITLE"] = "Development - ODC"
    cubedash.app.config["CUBEDASH_SISTER_SITES"] = (
        ("Production - ODC", "http://prod.odc.example"),
        ("Production - NCI", "http://nci.odc.example"),
    )
    cubedash.app.config["CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST"] = [
        "ls5_pq_scene",
        "ls7_pq_scene",
        "ls8_pq_scene",
        "ls5_pq_legacy_scene",
        "ls7_pq_legacy_scene",
    ]
    return client


@pytest.fixture()
def total_indexed_products_count(summary_store: SummaryStore):
    return len(list(summary_store.index.products.get_all()))


def test_instance_title(app_configured_client: FlaskClient):
    html = get_html(app_configured_client, "/about")

    instance_title = html.find(".instance-title", first=True).text
    assert instance_title == "Development - ODC"


def test_hide_products_audit_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
):
    html = get_html(app_configured_client, "/audit/storage")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "5"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 5) in h2


def test_hide_products_audit_bulk_dataset_display(
    app_configured_client: FlaskClient, total_indexed_products_count
):
    html = get_html(app_configured_client, "/audit/dataset-counts")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "5"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 5) in h2


def test_hide_products_product_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
):
    html = get_html(app_configured_client, "/products")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "5"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 5) in h2

    listed_product_count = html.find("tr.collapse-when-small")
    assert len(listed_product_count) == (total_indexed_products_count - 5)


def test_hide_products_menu_display(
    app_configured_client: FlaskClient, total_indexed_products_count
):
    html = get_html(app_configured_client, "/about")

    hide_products = html.find("#products-menu li a.configured-hide-product")
    assert len(hide_products) == 5

    products_hide_show_switch = html.find("a#show-hidden-product")
    assert products_hide_show_switch

    html = get_html(app_configured_client, "/products/dsm1sv10")
    products = html.find(".product-selection-header a.option-menu-link")
    assert total_indexed_products_count - len(products) == 5


def test_sister_sites(app_configured_client: FlaskClient):
    html = get_html(app_configured_client, "/about")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/about" in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )


def test_sister_sites_request_path(app_configured_client: FlaskClient):
    html = get_html(app_configured_client, "/products/ls5_fc_albers")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/products/ls5_fc_albers"
            in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )

    html = get_html(app_configured_client, "/products/ls5_fc_albers/datasets")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/products/ls5_fc_albers/datasets"
            in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )
