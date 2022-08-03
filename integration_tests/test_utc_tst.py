"""
Tests that load pages and check the contained text.
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from click.testing import Result

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
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "ls5_fc_albers-sample.yaml"):
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


def test_summary_product(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/ls5_fc_albers")

    check_dataset_count(html, 5)


def test_yearly_dataset_count(client: FlaskClient):
    html = get_html(client, "/ls5_fc_albers/2010")
    check_dataset_count(html, 2)

    html = get_html(client, "/ls5_fc_albers/2011")
    check_dataset_count(html, 3)


def test_clirunner_generate(unpopulated_client: FlaskClient, run_generate):
    res: Result = run_generate("ls5_fc_albers", grouping_time_zone="America/Chicago")
    assert "2010" in res.output

    html = get_html(unpopulated_client, "/ls5_fc_albers/2010")
    # check_dataset_count(html, 5)

    html = get_html(unpopulated_client, "/ls5_fc_albers/2010/12/31")
    # check_dataset_count(html, 5)

    html = get_html(unpopulated_client, "/products/ls5_fc_albers/datasets?time-begin=2010-12-31&time-end=2011-01-01")
    search_results = html.find(".search-result a")
    assert len(search_results) == 3