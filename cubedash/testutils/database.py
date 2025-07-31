import configparser
import os
import time
from collections import Counter
from pathlib import Path

import docker
import psycopg2
import psycopg2.extensions
import pytest
from datacube import Datacube
from datacube.cfg import ODCConfig, ODCEnvironment
from datacube.drivers.postgis import _core as pgis_core
from datacube.drivers.postgres import _core as pgres_core
from datacube.index import index_connect
from datacube.index.hl import Doc2Dataset
from datacube.model import MetadataType
from datacube.utils import read_documents
from datacube.utils.documents import InvalidDocException, UnknownMetadataType
from sqlalchemy import text

GET_DB_FROM_ENV = "get-the-db-from-the-environment-variable"


@pytest.fixture(scope="session")
def postgresql_server():
    """
    Provide a temporary PostgreSQL server for the test session using Docker.

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
        yield GET_DB_FROM_ENV
    else:
        client = docker.from_env()
        container = client.containers.run(
            "postgis/postgis:16-3.4",
            auto_remove=True,
            remove=True,
            detach=True,
            environment={
                "POSTGRES_PASSWORD": "badpassword",
                "POSTGRES_USER": "explorer_test",
            },
            ports={"5432/tcp": None},
        )
        try:
            while not container.attrs["NetworkSettings"]["Ports"]:
                time.sleep(1)
                container.reload()
            host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
                "HostPort"
            ]
            # From the documentation for the postgres docker image. The value of POSTGRES_USER
            # is used for both the user and the default database.
            yield {
                "db_hostname": "127.0.0.1",
                "db_username": "explorer_test",
                "db_port": host_port,
                "db_database": "explorer_test",
                "db_password": "badpassword",
                "index_driver": "default",
            }
        finally:
            container.remove(v=True, force=True)


@pytest.fixture(scope="module")
def odc_db(postgresql_server, tmp_path_factory, request):
    if postgresql_server == GET_DB_FROM_ENV:
        yield None  # os.environ["DATACUBE_DB_URL"]
    else:
        postgres_url = "postgresql://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_database}".format(
            **postgresql_server
        )

        new_db_database = request.module.__name__.replace(".", "_")
        # Wait for PostgreSQL Server to start up
        while True:
            try:
                conn = psycopg2.connect(postgres_url)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                with conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE {new_db_database};")
                break
            except psycopg2.OperationalError:
                print("Waiting for PostgreSQL to become available")
                time.sleep(1)

        postgresql_server["db_database"] = new_db_database
        temp_datacube_config_file = (
            tmp_path_factory.mktemp("odc") / "test_datacube.conf"
        )
        config = configparser.ConfigParser()
        config["default"] = postgresql_server
        postgresql_server["index_driver"] = "postgis"
        config["postgis"] = postgresql_server
        with open(temp_datacube_config_file, "w", encoding="utf8") as fout:
            config.write(fout)
        # Use pytest.MonkeyPatch instead of the monkeypatch fixture
        # to enable this fixture to not be function scoped
        mp = pytest.MonkeyPatch()

        mp.setenv(
            "ODC_CONFIG_PATH",
            str(temp_datacube_config_file.absolute()),
        )
        yield postgres_url
        mp.undo()


@pytest.fixture(scope="module", params=["default", "postgis"])
def env_name(request) -> str:
    return request.param


@pytest.fixture(scope="module")
def cfg_env(odc_db, env_name) -> ODCEnvironment:
    """Provides a :class:`ODCEnvironment` configured with suitable config file paths."""
    return ODCConfig()[env_name]


@pytest.fixture(scope="module")
def odc_test_db(cfg_env):
    """
    Provide a temporary PostgreSQL server initialised by ODC, usable as
    the default ODC DB by setting environment variables.
    :return: Datacube instance
    """

    index = index_connect(cfg_env, validate_connection=False)
    index.init_db()

    dc = Datacube(index=index)

    # Disable PostgreSQL Table logging. We don't care about storage reliability
    # during testing, and need any performance gains we can get.

    with index._db._engine.begin() as conn:
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

            # This actually drops the schema, not the DB
            pgres_core.drop_db(conn)  # pylint:disable=protected-access

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

            pgis_core.drop_db(conn)  # pylint:disable=protected-access

            _remove_postgis_dynamic_indexes()


def _remove_postgres_dynamic_indexes() -> None:
    """
    Clear any dynamically created postgresql indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    for table in pgres_core.METADATA.tables.values():
        table.indexes.intersection_update(
            [i for i in table.indexes if not i.name.startswith("dix_")]
        )


def _remove_postgis_dynamic_indexes() -> None:
    """
    Clear any dynamically created postgis indexes from the schema.
    """
    # Our normal indexes start with "ix_", dynamic indexes with "dix_"
    # for table in pgis_core.METADATA.tables.values():
    #    table.indexes.intersection_update([i for i in table.indexes if not i.name.startswith('dix_')])
    # Dynamic indexes disabled.


@pytest.fixture(scope="module")
def auto_odc_db(odc_test_db, request):
    """
    Load sample data into an ODC PostgreSQL Database for tests within a module.

    This fixture will look for global variables within the test module named,
    `METADATA_TYPES`, `PRODUCTS`, and `DATASETS`, which should be a list of filenames
    with a `data/` directory relative to the test module. These files will be added
    to the current ODC DB, defined by environment variables in the `odc_test_db`
    fixture.

    The fixture makes available a dict, keyed by name, counting the number of datasets
    added, not including derivatives.
    """
    odc_test_db.index.metadata_types.check_field_indexes(
        allow_table_lock=True,
        rebuild_indexes=False,
        rebuild_views=True,
    )
    data_path = request.path.parent.joinpath("data")
    if hasattr(request.module, "METADATA_TYPES"):
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

    dataset_count = Counter()
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
