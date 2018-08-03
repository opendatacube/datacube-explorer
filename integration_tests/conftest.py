from pathlib import Path

import pytest
from click.testing import CliRunner

from cubedash import generate, logs
from cubedash.summary import SummaryStore
from datacube.index import Index
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


@pytest.fixture(scope="function")
def summary_store(module_dea_index: Index) -> SummaryStore:
    SummaryStore.create(module_dea_index, init_schema=False).drop_all()
    store = SummaryStore.create(module_dea_index, init_schema=True)
    return store


@pytest.fixture(scope="function")
def summariser(summary_store: SummaryStore):
    return summary_store._summariser


@pytest.fixture(autouse=True, scope="session")
def init_logs(pytestconfig):
    logs.init_logging(verbose=pytestconfig.getoption("verbose") > 0)


@pytest.fixture
def tmppath(tmpdir):
    return Path(str(tmpdir))


@pytest.fixture
def clirunner(global_integration_cli_args):
    def _run_cli(cli_method, opts, catch_exceptions=False, expect_success=True):
        exe_opts = list(global_integration_cli_args)
        exe_opts.extend(opts)

        runner = CliRunner()
        result = runner.invoke(cli_method, exe_opts, catch_exceptions=catch_exceptions)
        if expect_success:
            assert 0 == result.exit_code, "Error for %r. output: %r" % (
                opts,
                result.output,
            )
        return result

    return _run_cli


@pytest.fixture()
def run_generate(clirunner, summary_store):
    def do(*only_products, expect_success=True):
        products = only_products or ["--all"]
        res = clirunner(generate.cli, products, expect_success=expect_success)
        return res

    return do
