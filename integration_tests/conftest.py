from pathlib import Path
from typing import Tuple

import pytest
from click.testing import CliRunner
from flask.testing import FlaskClient

import cubedash
from cubedash import _model, generate, logs
from cubedash.summary import SummaryStore
from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset
from datacube.utils import read_documents
from digitalearthau.testing import factories

# Use module-scoped databases, as it takes a while to populate with
# our data, and we're treating it as read-only in tests.
# -> Note: Since we're reusing the default config unchanged, we can't use the
#          default index/dea_index fixtures, as they'll override data from
#          the same db.
module_db = factories.db_fixture("local_config", scope="module")
module_index = factories.index_fixture("module_db", scope="module")
module_dea_index = factories.dea_index_fixture("module_index", scope="module")

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="function")
def summary_store(module_dea_index: Index) -> SummaryStore:
    SummaryStore.create(module_dea_index, init_schema=False).drop_all()
    module_dea_index.close()
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


@pytest.fixture(scope="module")
def dataset_loader(module_dea_index):
    def _populate_from_dump(expected_type: str, dump_path: Path):
        ls8_nbar_scene = module_dea_index.products.get_by_name(expected_type)
        dataset_count = 0

        create_dataset = Doc2Dataset(module_dea_index)

        for _, doc in read_documents(dump_path):
            label = doc["ga_label"] if ("ga_label" in doc) else doc["id"]
            # type: Tuple[Dataset, str]
            dataset, err = create_dataset(
                doc, f"file://example.com/test_dataset/{label}"
            )
            assert dataset is not None, err
            assert dataset.type.name == expected_type
            created = module_dea_index.datasets.add(dataset)

            assert created.type.name == ls8_nbar_scene.name
            dataset_count += 1

        print(f"Populated {dataset_count} of {expected_type}")
        return dataset_count

    return _populate_from_dump


@pytest.fixture(scope="function")
def empty_client(summary_store: SummaryStore) -> FlaskClient:
    _model.cache.clear()
    _model.STORE = summary_store
    cubedash.app.config["TESTING"] = True
    return cubedash.app.test_client()


@pytest.fixture(scope="function")
def unpopulated_client(
    empty_client: FlaskClient, summary_store: SummaryStore
) -> FlaskClient:
    _model.STORE.refresh_all_products()
    return empty_client


@pytest.fixture(scope="function")
def client(unpopulated_client: FlaskClient) -> FlaskClient:
    for product in _model.STORE.index.products.get_all():
        _model.STORE.get_or_update(product.name)
    return unpopulated_client


@pytest.fixture(scope="module")
def populated_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    loaded = dataset_loader("wofs_albers", TEST_DATA_DIR / "wofs-albers-sample.yaml.gz")
    assert loaded == 11

    loaded = dataset_loader(
        "high_tide_comp_20p", TEST_DATA_DIR / "high_tide_comp_20p.yaml.gz"
    )
    assert loaded == 306

    # These have very large footprints, as they were unioned from many almost-identical
    # polygons and not simplified. They will trip up postgis if used naively.
    # (postgis gist index has max record size of 8k per entry)
    loaded = dataset_loader(
        "pq_count_summary", TEST_DATA_DIR / "pq_count_summary.yaml.gz"
    )
    assert loaded == 20

    return module_dea_index
