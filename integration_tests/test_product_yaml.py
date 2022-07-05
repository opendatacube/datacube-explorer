
from flask.testing import FlaskClient

import datacube.scripts.cli_app
import datacube.ui.click

import tempfile


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


def test_low_complication_product_yaml_is_valid(client: FlaskClient, clirunner):
    response = client.get("/products/ls5_fc_albers.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)
    f.write(response.data)
    assert type(f.name) == str

    result = clirunner(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name,
        ],
        expect_success=False,
    )

    assert result.output.endswith('Updated "ls5_fc_albers"\n')
    assert result.exit_code == 0


def test_simple_product_yaml_is_valid(client: FlaskClient, clirunner):
    response = client.get("/products/dsm1sv10.odc-product.yaml", follow_redirects=True)
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

    assert result.output.endswith('Updated "dsm1sv10"\n')
    assert result.exit_code == 0
