import configparser
import os
import time
from collections import Counter
from collections.abc import Generator
from pathlib import Path

# There is a docker directory in the root, skip sorting that import until
# this repository uses a src layout.
import docker  # isort: skip
import psycopg2
import pytest
import structlog
from datacube import Datacube
from datacube.cfg import ODCConfig, ODCEnvironment
from datacube.drivers.postgis import _core as pgis_core
from datacube.drivers.postgres import _core as pgres_core
from datacube.index import index_connect
from datacube.index.hl import Doc2Dataset
from datacube.model import MetadataType
from datacube.utils import read_documents
from datacube.utils.documents import InvalidDocException, UnknownMetadataType
from psycopg2 import sql
from sqlalchemy import text

logger: structlog.BoundLogger = structlog.get_logger(__name__)


GET_DB_FROM_ENV = "get-the-db-from-the-environment-variable"
POSTGIS_IMAGE = "postgis/postgis:16-3.5"


@pytest.fixture(scope="session")
def postgresql_server(worker_id: str):
    """Provide a temporary PostgreSQL server for the test session using Docker.

    If already running inside Docker, and there's an ODC database configured with
    environment variables, do nothing.

    :return: ODC style dictionary configuration required to connect to the server
    """
    # If we're running inside docker already, don't attempt to start a container!
    # Hopefully we're using the `with-test-db` script and can use *that* database.
    # I think this may be copypasta from odc-tools
    if Path("/.dockerenv").exists() and (
        "ODC_DEFAULT_DB_URL" in os.environ or "ODC_POSTGIS_DB_URL" in os.environ
    ):
        logger.warning("Running inside Docker, not starting PostGIS container")
        yield GET_DB_FROM_ENV
    else:
        client = docker.from_env()
        logger.info("Starting PostGIS Docker Container", image=POSTGIS_IMAGE)
        postgres_args = [
            # Memory settings (helpful even with tmpfs)
            "-c",
            "shared_buffers=256MB",
            "-c",
            "work_mem=4MB",
            "-c",
            "maintenance_work_mem=64MB",
            # Skip WAL archiving and replication setup
            "-c",
            "wal_level=minimal",
            "-c",
            "max_wal_senders=0",
            "-c",
            "archive_mode=off",
            # These are redundant with tmpfs but included for completeness
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
        ]
        container = client.containers.run(
            POSTGIS_IMAGE,
            name=f"cubedash-testdb-{worker_id}",
            command=postgres_args,
            auto_remove=True,
            remove=True,
            detach=True,
            environment={
                "POSTGRES_PASSWORD": "explorer_test_password",
                "POSTGRES_USER": "explorer_test_user",
                "POSTGRES_DB": "explorer_test_db",
                "POSTGRES_INITDB_ARGS": "--no-sync",  # Skip initdb's fsync for faster initial setup
                "PGDATA": "/tmp/explorertest/data",
            },
            tmpfs={
                "/tmp/explorertest": "rw,noexec,nosuid,noatime,nodiratime",  # ,size=512m"  # Use RAM drive for storage
                # ,size=1g
            },
            ports={"5432/tcp": None},
        )
        try:
            while not container.attrs["NetworkSettings"]["Ports"]:
                time.sleep(1)
                container.reload()
            host_port: str = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
                "HostPort"
            ]
            # From the documentation for the postgres docker image. The value of POSTGRES_USER
            # is used for both the user and the default database.
            db_url = f"postgresql://explorer_test_user:explorer_test_password@127.0.0.1:{host_port}/explorer_test_db"
            logger.info("Docker container started", db_url=db_url)
            yield db_url
        finally:
            logger.info("Removing PostGIS container")
            container.remove(v=True, force=True)


@pytest.fixture(scope="module")
def odc_db(
    postgresql_server: str,
    env_name: str,
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
    worker_id,
    testrun_uid,
) -> Generator[str | ODCEnvironment]:
    """Create an ODC Database per xdist worker and per pytest module

    Write out an ODC Configuration File and set an Environment Variable specifying which ODC Env to Use.

    We expose the current database by either writing out to a file (or, update a swath of environment variables)
    rather than having a fixture which works at the Python level (SQLAlchemy engine or ODC Index/Database object)
    because many of the integration tests work by launching the command line tools, which while not starting up
    a whole new Python process, don't share much of the existing state and load their ODC Connection parameters
    either from a file or environment variables.
    """
    if postgresql_server == GET_DB_FROM_ENV:
        yield ODCConfig()[env_name]
    else:
        log = logger.bind(
            db_url=postgresql_server, worker_id=worker_id, testrun_uid=testrun_uid
        )
        test_database_name = f"{request.path.stem}_{env_name}"
        # Wait for PostgreSQL Server to start up
        while True:
            try:
                conn = psycopg2.connect(postgresql_server)
                # Prevent starting a transaction, since 'create database' cannot run inside a transaction
                conn.set_session(autocommit=True)

                # assert not test_database_name.endswith('postgis')
                # if test_database_name.endswith('postgis'):
                # import web_pdb; web_pdb.set_trace()

                try:
                    with conn.cursor() as cur:
                        log.info("Creating database", db_name=test_database_name)
                        cur.execute(
                            sql.SQL("CREATE DATABASE {db_name}").format(
                                db_name=sql.Identifier(test_database_name)
                            )
                        )
                except psycopg2.errors.DuplicateDatabase:
                    log.warning(
                        "Database already exists, continuing",
                        db_name=test_database_name,
                    )
                    import web_pdb

                    web_pdb.set_trace()
                    # breakpoint()
                finally:
                    conn.close()
                break
            except psycopg2.OperationalError:
                log.info("Waiting for PostgreSQL to become available")
                time.sleep(1)

        new_postgresql_url = postgresql_server.replace(
            "explorer_test_db", test_database_name
        )

        log = log.bind(db_url=new_postgresql_url)
        log.info("Creating postgis extension")
        conn = psycopg2.connect(new_postgresql_url)
        # Prevent starting a transaction, since 'create extension' cannot run inside a transaction
        conn.set_session(autocommit=True)
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION postgis")
        conn.close()

        # Create a temporary configuration file with our database URL and index driver name
        config = configparser.ConfigParser()
        config[env_name] = {"index_driver": env_name, "db_url": new_postgresql_url}

        temp_datacube_config_file: Path = (
            tmp_path_factory.mktemp("odc") / "test_datacube.conf"
        )
        with temp_datacube_config_file.open("w", encoding="utf8") as fout:
            config.write(fout)

        # Use pytest.MonkeyPatch instead of the monkeypatch fixture
        # so that this fixture can be module scoped and not function scoped.
        mp = pytest.MonkeyPatch()

        # Share the temporary configuration file with the tests
        mp.setenv("ODC_CONFIG_PATH", str(temp_datacube_config_file.absolute()))
        yield new_postgresql_url
        mp.undo()

        # This was a good idea to save disk space on the tmpfs
        # But something is holding connections open to the database
        # E  psycopg2.errors.ObjectInUse: database "test_dataset_maturity_default" is being accessed
        # by other users
        # E  DETAIL:  There are 2 other sessions using the database.
        # conn = psycopg2.connect(postgresql_server)
        # # Prevent starting a transaction, since 'drop database' cannot run inside a transaction
        # conn.set_session(autocommit=True)
        # with conn.cursor() as cur:
        #     log.info("Dropping database", db_name=test_database_name)
        #     cur.execute(
        #         sql.SQL("DROP DATABASE {db_name}").format(
        #             db_name=sql.Identifier(test_database_name)
        #         )
        #     )
        # conn.close()


@pytest.fixture(scope="module", params=["default", "postgis"])
def env_name(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(scope="module")
def cfg_env(odc_db, env_name: str) -> ODCEnvironment:
    """Provide a :class:`ODCEnvironment` configured with suitable config file paths."""
    # Need to request `odc_db` since it exports an environment variable specifying which
    # configuration file to use. So this fixture *must* run after that fixture.
    odc_env = ODCConfig().get_environment(env_name)
    assert odc_env.db_url is not None
    logger.info(
        "Prepping ODCConfig",
        env_name=env_name,
        odc_env_url=odc_env.db_url,
    )
    return odc_env


@pytest.fixture(scope="module")
def odc_test_db(odc_db: str, cfg_env: ODCEnvironment) -> Generator[Datacube]:
    """Provide a temporary PostgreSQL server initialised by ODC.

    Usable as the default ODC DB by setting environment variables.

    Yields:
        Ready to go Datacube Object

    """
    logger.info(
        "Initialising ODC Schema for testing", odc_db=odc_db, cfg_db_url=cfg_env.db_url
    )
    index = index_connect(cfg_env, validate_connection=False)
    # With permissions causes trouble when run multiple times on the same database
    # Although, maybe not, since there should be a separate database every time.
    # But something is getting screwed up and it's getting a database name of 'datacube'.
    index.init_db()
    #  with_default_types=False, with_permissions=True

    dc = Datacube(index=index)

    logger.info("Database initialised", odc_dc=dc)

    # Disable PostgreSQL Table logging. We don't care about storage reliability
    # during testing, and need any performance gains we can get.

    with index._db._engine.begin() as conn:  # type: ignore[attr-defined]
        if index.name == "pg_index":
            for table in [
                "agdc.dataset_location",
                "agdc.dataset_source",
                "agdc.dataset",
                "agdc.dataset_type",
                "agdc.metadata_type",
            ]:
                conn.execute(text(f"alter table {table} set unlogged"))

            yield dc

            dc.close()

            # Drops the AGDC schema - badly named function
            pgres_core.drop_db(conn)

            # We need to run this as well, I think because SQLAlchemy grabs them into it's MetaData,
            # and attempts to recreate them.
            _remove_postgres_dynamic_indexes()
        else:
            for table in [
                "odc.dataset_lineage",
                "odc.dataset_search_string",
                "odc.dataset_search_num",
                "odc.dataset_search_datetime",
                "odc.spatial_indicies",
                "odc.spatial_4326",
                "odc.dataset",
                "odc.product",
                "odc.metadata_type",
            ]:
                conn.execute(text(f"alter table {table} set unlogged"))

            yield dc

            dc.close()

            # Drop the ODC Schema - Badly named function
            pgis_core.drop_db(conn)

            _remove_postgis_dynamic_indexes()


def _remove_postgres_dynamic_indexes() -> None:
    """Clear any dynamically created postgresql indexes from the schema."""
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgres_core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if i.name and not i.name.startswith("dix_")]
        )


def _remove_postgis_dynamic_indexes() -> None:
    """Clear any dynamically created postgis indexes from the schema."""
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgis_core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if not i.name.startswith("dix_")]  # type: ignore
        )
    # Dynamic indexes disabled.


@pytest.fixture(scope="module")
def auto_odc_db(odc_test_db: Datacube, request: pytest.FixtureRequest):
    """Load sample data into an ODC PostgreSQL Database for tests within a module.

    This fixture will look for global variables within the test module named,
    `METADATA_TYPES`, `PRODUCTS`, and `DATASETS`, which should be a list of filenames
    with a `data/` directory relative to the test module. These files will be added
    to the current ODC DB, defined by environment variables in the `odc_test_db`
    fixture.

    The fixture makes available a dict, keyed by name, counting the number of datasets
    added, not including derivatives.
    """
    log = logger.bind(
        current_db=odc_test_db.index.environment.db_url,
        current_dc=str(odc_test_db),
        module=request.module.__name__,
    )
    log.info("Preparing to load data into Test ODC Database")
    odc_test_db.index.metadata_types.check_field_indexes(
        allow_table_lock=True, rebuild_indexes=False, rebuild_views=True
    )
    data_path = request.path.parent.joinpath("data")
    if hasattr(request.module, "METADATA_TYPES"):
        log.info("Loading MetadataDocs for %s", request.module)
        for filename in request.module.METADATA_TYPES:
            filename = data_path / filename
            for _, meta_doc in read_documents(filename):
                try:
                    odc_test_db.index.metadata_types.add(MetadataType(meta_doc))
                except InvalidDocException:
                    # skip non-eo3 metadata/products/datasets when using the postgis index
                    continue

    if hasattr(request.module, "PRODUCTS"):
        for filename in request.module.PRODUCTS:
            filename = data_path / filename
            for _, prod_doc in read_documents(filename):
                try:
                    odc_test_db.index.products.add_document(prod_doc)
                except UnknownMetadataType:
                    continue

    dataset_count: Counter[str] = Counter()
    if hasattr(request.module, "DATASETS"):
        create_dataset = Doc2Dataset(odc_test_db.index)
        for filename in request.module.DATASETS:
            filename = data_path / filename
            for _, doc in read_documents(filename):
                label = doc["ga_label"] if ("ga_label" in doc) else doc["id"]
                try:
                    dataset, err = create_dataset(
                        doc, f"file://example.com/test_dataset/{label}"
                    )
                    assert dataset is not None, err
                    created = odc_test_db.index.datasets.add(dataset)
                    assert created.uri
                    dataset_count[created.product.name] += 1
                except ValueError:
                    continue

            print(f"Loaded Datasets: {dataset_count}")
    return dataset_count
