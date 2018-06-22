from pathlib import Path
from typing import Type

import pytest

from cubedash import logs
from cubedash.summary import FileSummaryStore, SummaryStore
from cubedash.summary._stores import PgSummaryStore
from datacube.index import Index
from datacube.scripts.dataset import create_dataset, load_rules_from_types
from datacube.utils import read_documents
from digitalearthau.testing import factories

pytest_plugins = "digitalearthau.testing.plugin"

# Use module-scoped databases, as it takes a while to populate with
# our data, and we're treating it as read-only in tests.
# -> Note: Since we're reusing the default config unchanged, we can't use the
#          default index/dea_index fixtures, as they'll override data from
#          the same db.
module_db = factories.db_fixture("local_config", scope="module")
module_index = factories.index_fixture("module_db", scope="module")
module_dea_index = factories.dea_index_fixture("module_index", scope="module")


@pytest.fixture(scope="function", params=["file_store", "db_store"])
def summary_store(module_dea_index: Index, tmppath: Path, request) -> SummaryStore:
    p = request.param
    if p == "file_store":
        store = FileSummaryStore(module_dea_index, tmppath)
    elif p == "db_store":
        store = PgSummaryStore(module_dea_index)
        store.drop_all()
        store.init()
    else:
        raise ValueError(f"Unknown store type {repr(p)}")

    return store


@pytest.fixture(autouse=True, scope="session")
def init_logs(pytestconfig):
    logs.init_logging(verbose=pytestconfig.getoption("verbose") > 0)


@pytest.fixture
def tmppath(tmpdir):
    return Path(str(tmpdir))
