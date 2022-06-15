"""
Tests that indexes DEA C3 Summary products region tiles
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from integration_tests.asserts import check_dataset_count, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ga_ls_wo_fq_nov_mar_3-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "ga_ls_wo_fq_nov_mar_3"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 5
            print(ae)
    assert dataset_count == 5
    return module_dea_index


def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/ga_ls_wo_fq_nov_mar_3")

    check_dataset_count(html, 5)


def test_region_dataset_count(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/product/ga_ls_wo_fq_nov_mar_3/regions/x11y46")

    search_results = html.find(".search-result a")
    assert len(search_results) == 5
