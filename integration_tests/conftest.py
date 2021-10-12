from contextlib import contextmanager
from pathlib import Path
from textwrap import indent
from typing import Tuple

import pytest
import sqlalchemy
import structlog
from click.testing import CliRunner
from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgres._core import METADATA as ODC_SCHEMA_METADATA
from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset
from datacube.utils import read_documents
from digitalearthau.testing import factories
from flask.testing import FlaskClient
from structlog import DropEvent

import cubedash
from cubedash import _model, _utils, generate, logs
from cubedash.summary import SummaryStore
from cubedash.summary._schema import METADATA as CUBEDASH_METADATA
from cubedash.warmup import find_examples_of_all_public_urls

# Use module-scoped databases, as it takes a while to populate with
# our data, and we're treating it as read-only in tests.
# -> Note: Since we're reusing the default config unchanged, we can't use the
#          default index/dea_index fixtures, as they'll override data from
#          the same db.
from integration_tests.asserts import format_doc_diffs

module_vanilla_db = factories.db_fixture("local_config", scope="module")


def pytest_assertrepr_compare(op, left, right):
    """
    Custom pytest error messages for large documents.

    The default pytest dict==dict error messages are unreadable for
    nested document-like dicts. (Such as our json and yaml docs!)

    We just want to know which fields differ.
    """

    def is_a_doc(o: object):
        """
        Is it a dict that's not printable on one line?
        """
        return isinstance(o, dict) and len(repr(o)) > 79

    if (is_a_doc(left) or is_a_doc(right)) and op == "==":
        return format_doc_diffs(left, right)


@pytest.fixture(scope="module")
def module_db(module_vanilla_db: PostgresDb) -> PostgresDb:
    # Set all the tables to unlogged for faster perf.
    _make_all_tables_unlogged(module_vanilla_db._engine, ODC_SCHEMA_METADATA)
    return module_vanilla_db


def _make_all_tables_unlogged(engine, metadata: sqlalchemy.MetaData):
    """
    Set all tables in this alchemy metadata to unlogged.

    Make them faster, but data is lost on crashes. Which is a good
    trade-off for tests.
    """
    for table in reversed(metadata.sorted_tables):
        table: sqlalchemy.Table
        if table.name.startswith("mv_"):
            # Not supported for materialised views.
            continue
        else:
            engine.execute(f"""alter table {table.selectable.fullname} set unlogged;""")


module_index = factories.index_fixture("module_db", scope="module")

module_dea_index = factories.dea_index_fixture("module_index", scope="module")

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture()
def summary_store(module_dea_index: Index) -> SummaryStore:
    store = SummaryStore.create(module_dea_index)
    store.drop_all()
    module_dea_index.close()

    with disable_logging():
        # Some CRS/storage tests use test data that is 3577
        store.init(grouping_epsg_code=3577)

    _make_all_tables_unlogged(
        _utils.alchemy_engine(module_dea_index), CUBEDASH_METADATA
    )
    return store


@pytest.fixture()
def summariser(summary_store: SummaryStore):
    return summary_store._summariser


@pytest.fixture(autouse=True, scope="session")
def _init_logs(pytestconfig):
    logs.init_logging(
        verbosity=pytestconfig.getoption("verbose"), cache_logger_on_first_use=False
    )


@pytest.fixture()
def tmppath(tmpdir):
    return Path(str(tmpdir))


@pytest.fixture()
def clirunner(global_integration_cli_args):
    def _run_cli(cli_method, opts, catch_exceptions=False, expect_success=True):
        exe_opts = list(global_integration_cli_args)
        exe_opts.extend(opts)

        runner = CliRunner()
        result = runner.invoke(cli_method, exe_opts, catch_exceptions=catch_exceptions)
        if expect_success:
            assert (
                0 == result.exit_code
            ), f"Error for {opts}. Out:\n{indent(result.output, ' ' * 4)}"
        return result

    return _run_cli


@pytest.fixture()
def run_generate(clirunner, summary_store, multi_processed=False):
    def do(*args, expect_success=True):
        args = args or ("--all",)
        if not multi_processed:
            args = ("-j", "1") + tuple(args)
        res = clirunner(generate.cli, args, expect_success=expect_success)
        return res

    return do


@pytest.fixture(scope="module")
def dataset_loader(module_dea_index: Index):
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
            assert created.uris
            assert created.type.name == ls8_nbar_scene.name
            dataset_count += 1

        print(f"Populated {dataset_count} of {expected_type}")
        return dataset_count

    return _populate_from_dump


@pytest.fixture()
def all_urls(summary_store: SummaryStore):
    """A list of public URLs to try on the current Explorer instance"""
    return list(find_examples_of_all_public_urls(summary_store.index))


@pytest.fixture()
def empty_client(summary_store: SummaryStore) -> FlaskClient:
    _model.cache.clear()
    _model.STORE = summary_store
    cubedash.app.config["TESTING"] = True
    return cubedash.app.test_client()


@pytest.fixture()
def unpopulated_client(
    empty_client: FlaskClient, summary_store: SummaryStore
) -> FlaskClient:
    with disable_logging():
        _model.STORE.refresh_all_product_extents()
    return empty_client


@contextmanager
def disable_logging():
    """
    Turn off logging within the if-block

    Used for repetitive environment setup that makes test errors too verbose.
    """
    original_processors = structlog.get_config()["processors"]

    def swallow_log(_logger, _log_method, _event_dict):
        raise DropEvent

    structlog.configure(processors=[swallow_log])
    try:
        yield
    finally:
        structlog.configure(processors=original_processors)


@pytest.fixture()
def client(unpopulated_client: FlaskClient) -> FlaskClient:

    with disable_logging():
        for product in _model.STORE.index.products.get_all():
            _model.STORE.refresh(product.name)

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
