
from flask.testing import FlaskClient

import datacube.scripts.cli_app
import datacube.ui.click

import tempfile
import pytest


def test_product_yaml_with_scientific_notation_is_valid(client: FlaskClient, clirunner):
    response = client.get('products/ga_s2a_ard_nbar_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)
    f.write(response.data)

    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name,
        ],
    )

    assert result.output.endswith('Updated "ga_s2a_ard_nbar_granule"\n')
    assert result.exit_code == 0


def test_wagl_product_yaml_is_valid(client: FlaskClient, clirunner):
    response = client.get('products/s2a_ard_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)
    f.write(response.data)

    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name,
        ]
    )

    assert result.output.endswith('Updated "s2a_ard_granule"\n')
    assert result.exit_code == 0


@pytest.fixture()
def product_yaml_from_raw(client):
    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)

    response = client.get("/products/ls5_fc_albers.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)

    response = client.get("/products/dsm1sv10.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f.write(response.data)
    return f.name


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
    assert result.exit_code == 0
