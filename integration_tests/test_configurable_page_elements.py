from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

import cubedash
from cubedash.summary import SummaryStore
from integration_tests.asserts import get_html


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


TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(
        TEST_DATA_DIR / "ls5_fc_albers-sample.yaml"
    ):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ls5_fc_albers"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 5
    return module_dea_index


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
