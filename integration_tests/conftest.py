import pytest
from pathlib import Path

from cubedash import logs
from cubedash.summary import FileSummaryStore
from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from digitalearthau.testing import factories

pytest_plugins = "digitalearthau.testing.plugin"

TEST_DATA_DIR = Path(__file__).parent / 'data'

# Use session-scoped databases, as it takes a while to populate with
# our data, and we're treating it as read-only in tests.
# -> Note: Since we're reusing the default config unchanged, we can't use the
#          default index/dea_index fixtures, as they'll override data from
#          the same db.
session_db = factories.db_fixture('local_config', scope='session')
session_index = factories.index_fixture('session_db', scope='session')
session_dea_index = factories.dea_index_fixture('session_index', scope='session')


@pytest.fixture(scope='session')
def populated_index(session_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's session-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        session_dea_index,
        'ls8_nbar_scene',
        TEST_DATA_DIR / 'ls8-nbar-scene-sample-2017.yaml.gz'
    )
    _populate_from_dump(
        session_dea_index,
        'ls8_nbar_albers',
        TEST_DATA_DIR / 'ls8-nbar-albers-sample.yaml.gz'
    )
    return session_dea_index


@pytest.fixture(scope='function')
def summary_store(populated_index: Index, tmppath: Path):
    return FileSummaryStore(populated_index, tmppath)


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


@pytest.fixture(autouse=True, scope='session')
def init_logs(pytestconfig):
    logs.init_logging(
        verbose=pytestconfig.getoption('verbose') > 0
    )


@pytest.fixture
def tmppath(tmpdir):
    return Path(str(tmpdir))
