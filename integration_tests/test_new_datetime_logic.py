"""
Please note, in this test case, one of the dataset has datetime within
end_datetime and start_datetime range (actual live dataset sample)
while the other dataset has datetime outside of
end_datetime and start_datetime range (deliberate test setup sample)
"""
from pathlib import Path

import pytest
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from flask.testing import FlaskClient

from integration_tests.asserts import check_dateset_datestring_for, get_html

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    dataset_count = 0
    create_dataset = Doc2Dataset(module_dea_index)
    for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "rainfall_chirps_daily-sample.yaml"):
        try:
            dataset, err = create_dataset(
                s2_dataset_doc, "file://example.com/test_dataset/"
            )
            assert dataset is not None, err
            created = module_dea_index.datasets.add(dataset)
            assert created.type.name == "rainfall_chirps_daily"
            dataset_count += 1
        except AttributeError as ae:
            assert dataset_count == 2
            print(ae)
    assert dataset_count == 2
    return module_dea_index


def test_datestring_on_dataset_page(client: FlaskClient):
    # These datasets have gigantic footprints that can trip up postgis.
    html = get_html(client, "/products/rainfall_chirps_daily/datasets/35cbccee-cb07-51cf-85d2-6d2948957544")

    check_dateset_datestring_for(html, "31st May 2019")

    html = get_html(client, "/products/rainfall_chirps_daily/datasets/35cbccee-cb07-51cf-85d2-6d2948957545")

    check_dateset_datestring_for(html, "15th May 2019")
