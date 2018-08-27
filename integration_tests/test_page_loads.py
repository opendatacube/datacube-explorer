from pathlib import Path

import pytest
from dateutil import tz
from flask import Response
from flask.testing import FlaskClient

import cubedash
from cubedash import _model
from cubedash.summary import SummaryStore
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents

TEST_DATA_DIR = Path(__file__).parent / 'data'

DEFAULT_TZ = tz.gettz('Australia/Darwin')


def _populate_from_dump(session_dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = session_dea_index.products.get_by_name(expected_type)
    dataset_count = 0

    create_dataset = Doc2Dataset(session_dea_index)

    for _, doc in read_documents(dump_path):
        label = doc['ga_label'] if ('ga_label' in doc) else doc['id']
        dataset, err = create_dataset(doc, f"file://example.com/test_dataset/{label}")
        assert dataset is not None, err
        created = session_dea_index.datasets.add(dataset)

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(scope='module', autouse=True)
def populate_index(module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        module_dea_index,
        'wofs_albers',
        TEST_DATA_DIR / 'wofs-albers-sample.yaml.gz'
    )
    return module_dea_index


@pytest.fixture(scope='function')
def cubedash_client(summary_store: SummaryStore) -> FlaskClient:
    _model.STORE = summary_store
    _model.STORE.refresh_all_products()
    for product in summary_store.index.products.get_all():
        _model.STORE.get_or_update(product.name)

    cubedash.app.config['TESTING'] = True
    return cubedash.app.test_client()


def test_default_redirect(cubedash_client: FlaskClient):
    client = cubedash_client
    rv: Response = client.get('/', follow_redirects=False)
    # Redirect to a default.
    assert rv.location.endswith('/ls7_nbar_scene')


def test_get_overview(cubedash_client: FlaskClient):
    client = cubedash_client

    rv: Response = client.get('/wofs_albers')
    assert b'11 datasets' in rv.data
    i = rv.data.find(b'Last processed <time')
    print(rv.data[i:i + 62])
    assert b'Last processed <time datetime=2018-05-20T11:25:35' in rv.data
    assert b'Historic Flood Mapping Water Observations from Space' in rv.data

    rv: Response = client.get('/wofs_albers/2017')

    assert b'11 datasets' in rv.data
    assert b'Last processed <time datetime=2018-05-20T11:25:35' in rv.data
    assert b'Historic Flood Mapping Water Observations from Space' in rv.data

    rv: Response = client.get('/wofs_albers/2017/04')
    assert b'4 datasets' in rv.data
    assert b'Last processed <time datetime=2018-05-20T09:36:57' in rv.data
    assert b'Historic Flood Mapping Water Observations from Space' in rv.data


def test_view_dataset(cubedash_client: FlaskClient):

    # ls7_level1_scene dataset
    rv: Response = cubedash_client.get("/dataset/57848615-2421-4d25-bfef-73f57de0574d")
    # Label of dataset is header
    assert b'<h2>LS7_ETM_OTH_P51_GALPGS01-002_105_074_20170501</h2>' in rv.data

    # wofs_albers dataset (has no label or location)
    rv: Response = cubedash_client.get("/dataset/20c024b5-6623-4b06-b00c-6b5789f81eeb")
    assert b'-20.502 to -19.6' in rv.data
    assert b'132.0 to 132.924' in rv.data


def test_view_product(cubedash_client: FlaskClient):
    rv: Response = cubedash_client.get("/product/ls7_nbar_scene")
    assert b'Landsat 7 NBAR 25 metre' in rv.data


def test_about_page(cubedash_client: FlaskClient):
    rv: Response = cubedash_client.get("/about")
    assert b"wofs_albers" in rv.data
    assert b'11 total datasets' in rv.data


@pytest.mark.skip(reason="TODO: fix out-of-date range return value")
def test_out_of_date_range(cubedash_client: FlaskClient):
    """
    We have generated summaries for this product, but the date is out of the product's date range.
    """
    client = cubedash_client

    rv: Response = client.get('/wofs_albers/2010')
    assert rv.status_code == 200
    print(rv.data.decode('utf-8'))
    # The common error here is to say "No data: not yet generated" rather than "0 datasets"
    assert b'0 datasets' in rv.data
    assert b'Historic Flood Mapping Water Observations from Space' in rv.data


def test_no_data_pages(cubedash_client: FlaskClient):
    """
    Fetch products that exist but have no summaries generated.

    (these should load with "empty" messages: not throw exceptions)
    """
    client = cubedash_client

    rv: Response = client.get('/ls8_nbar_albers/2017')
    assert rv.status_code == 200
    assert b'No data: not yet generated' in rv.data
    assert b'Unknown number of datasets' in rv.data

    rv: Response = client.get('/ls8_nbar_albers/2017/5')
    assert rv.status_code == 200
    assert b'No data: not yet generated' in rv.data
    assert b'Unknown number of datasets' in rv.data

    # Days are generated on demand: it should query and see that there are no datasets.
    rv: Response = client.get('/ls8_nbar_albers/2017/5/2')
    assert rv.status_code == 200
    assert b'0 datasets' in rv.data


def test_missing_dataset(cubedash_client: FlaskClient):
    rv: Response = cubedash_client.get('/datasets/f22a33f4-42f2-4aa5-9b20-cee4ca4a875c')
    assert rv.status_code == 404


def test_invalid_product(cubedash_client: FlaskClient):
    rv: Response = cubedash_client.get('/fake_test_product/2017')
    assert rv.status_code == 404
