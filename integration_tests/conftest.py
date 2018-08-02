import pytest
from pathlib import Path

from cubedash import logs
from cubedash.summary import SummaryStore
from datacube.index import Index
from digitalearthau.testing import factories

pytest_plugins = "digitalearthau.testing.plugin"

# Use module-scoped databases, as it takes a while to populate with
# our data, and we're treating it as read-only in tests.
# -> Note: Since we're reusing the default config unchanged, we can't use the
#          default index/dea_index fixtures, as they'll override data from
#          the same db.
module_db = factories.db_fixture('local_config', scope='module')
module_index = factories.index_fixture('module_db', scope='module')
module_dea_index = factories.dea_index_fixture('module_index', scope='module')


@pytest.fixture(scope='function')
def summary_store(module_dea_index: Index) -> SummaryStore:
    store = SummaryStore.create(module_dea_index)
    store.drop_all()
    store.init()
    return store


@pytest.fixture(scope='function')
def summariser(summary_store: SummaryStore):
    return summary_store._summariser


@pytest.fixture(autouse=True, scope='session')
def init_logs(pytestconfig):
    logs.init_logging(
        verbose=pytestconfig.getoption('verbose') > 0
    )


@pytest.fixture
def tmppath(tmpdir):
    return Path(str(tmpdir))
