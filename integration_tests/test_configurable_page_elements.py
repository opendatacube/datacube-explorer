import pytest
from flask.testing import FlaskClient

from cubedash.summary import SummaryStore
from integration_tests.asserts import get_html

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
def app_configured_client(client: FlaskClient):
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
        }
    )
    return client


@pytest.fixture()
def total_indexed_products_count(summary_store: SummaryStore):
    return len(list(summary_store.index.products.get_all()))


def test_instance_title(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/about")

    instance_title = html.find(".instance-title", first=True).text
    assert instance_title == "Development - ODC"


def test_hide_products_audit_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/audit/storage")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "3"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 3) in h2


def test_hide_products_audit_bulk_dataset_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/audit/dataset-counts")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "3"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 3) in h2


def test_hide_products_product_page_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/products")
    hidden_product_count = html.find("span.hidden-product-count", first=True).text
    assert hidden_product_count == "3"

    h2 = html.find("h2", first=True).text
    indexed_product_count = html.find("span.indexed-product-count", first=True).text
    assert indexed_product_count == str(total_indexed_products_count)
    assert str(total_indexed_products_count - 3) in h2

    listed_product_count = html.find("tr.collapse-when-small")
    assert len(listed_product_count) == (total_indexed_products_count - 3)


def test_hide_products_menu_display(
    app_configured_client: FlaskClient, total_indexed_products_count
) -> None:
    html = get_html(app_configured_client, "/about")

    hide_products = html.find("#products-menu li a.configured-hide-product")
    assert len(hide_products) == 3

    products_hide_show_switch = html.find("a#show-hidden-product")
    assert products_hide_show_switch

    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3")
    products = html.find(".product-selection-header a.option-menu-link")
    assert total_indexed_products_count - len(products) == 3


def test_sister_sites(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/about")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/about" in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )


def test_sister_sites_request_path(app_configured_client: FlaskClient) -> None:
    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/products/ga_ls5t_ard_3"
            in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )

    html = get_html(app_configured_client, "/products/ga_ls5t_ard_3/datasets")

    sister_instances = html.find("#sister-site-menu ul li")
    assert len(sister_instances) == 2

    for sister_instance in sister_instances:
        assert (
            "/products/ga_ls5t_ard_3/datasets"
            in sister_instance.find("a.sister-link", first=True).attrs["href"]
        )
