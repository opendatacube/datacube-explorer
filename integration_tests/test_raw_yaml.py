
"""
Tests rendered raw yaml pages by passing the rendered content to datacube cli to validate:
- odc-type.yaml (cli command: datacube metadata)
- odc-product.yaml (cli command: datacube product)
- odc-metadata.yaml (cli command: datacube dataset)
"""
from flask.testing import FlaskClient
from pathlib import Path

import datacube.scripts.cli_app

import tempfile
import pytest

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module", autouse=True)
def populate_index(dataset_loader, module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    loaded = dataset_loader(
        "s2a_ard_granule", TEST_DATA_DIR / "s2a_ard_granule.yaml.gz"
    )
    assert loaded == 8
    return module_dea_index


@pytest.fixture()
def type_yaml_from_raw(client: FlaskClient):
    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)

    # odc-type definition
    response = client.get("/metadata-types/eo_plus.odc-type.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    return f.name


@pytest.fixture()
def product_yaml_from_raw(client: FlaskClient):
    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)

    # low complex product definition
    response = client.get("/products/ls5_fc_albers.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    # simple product definition
    response = client.get("/products/dsm1sv10.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    # wagl product definition
    response = client.get('products/s2a_ard_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    # high complex product definition with scientific notation
    response = client.get('products/ga_s2a_ard_nbar_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    return f.name


@pytest.fixture()
def dataset_yaml_from_raw(client: FlaskClient):
    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)

    # odc-type definition
    response = client.get("/dataset/290eca22-defc-43b4-998f-eaf56e1fd211.odc-metadata.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    return f.name


def test_update_type(type_yaml_from_raw, clirunner):
    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "metadata",
            "update",
            type_yaml_from_raw,
        ],
        expect_success=False,
    )

    assert 'Updated "eo_plus"\n' in result.output
    assert result.exit_code == 0


def test_update_product(product_yaml_from_raw, clirunner):
    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            product_yaml_from_raw,
        ],
        expect_success=False,
    )

    assert 'Updated "ls5_fc_albers"\n' in result.output
    assert 'Updated "dsm1sv10"\n' in result.output
    assert 'Updated "ga_s2a_ard_nbar_granule"\n' in result.output
    assert 'Updated "s2a_ard_granule"\n' in result.output
    assert result.exit_code == 0


def test_update_dataset(dataset_yaml_from_raw, clirunner):
    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "dataset",
            "update",
            dataset_yaml_from_raw,
        ],
        expect_success=False,
    )

    assert 'Updated 290eca22-defc-43b4-998f-eaf56e1fd211\n1 successful, 0 failed\n' in result.output
    assert result.exit_code == 0