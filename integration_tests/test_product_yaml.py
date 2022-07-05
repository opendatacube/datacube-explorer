
from flask.testing import FlaskClient
from io import StringIO
from pathlib import Path

from cubedash import _model
from ruamel.yaml import YAML
import yaml

from datacube.utils.serialise import SafeDatacubeDumper

import datacube.scripts.cli_app
from integration_tests.asserts import (
    check_dataset_count,
    get_html,
    get_text_response
)
from datacube.index import Index
from datacube.utils import read_documents
import pytest
import re
import tempfile
from datacube.utils import read_documents, InvalidDocException

from click.testing import CliRunner


def test_product_yaml_with_scientific_notation_is_valid(client: FlaskClient):
    response = client.get('products/ga_s2a_ard_nbar_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml')
    f.write(response.data)

    runner = CliRunner()

    result = runner.invoke(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name
        ]
    )

    assert result.output == 'Updated "ga_s2a_ard_nbar_granule"\n'
    assert result.exit_code == 0


def test_wagl_product_yaml_is_valid(client: FlaskClient):
    response = client.get('products/s2a_ard_granule.odc-product.yaml')
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml')
    f.write(response.data)

    runner = CliRunner()

    result = runner.invoke(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name
        ]
    )

    assert result.output == 'Updated "s2a_ard_granule"\n'
    assert result.exit_code == 0


@pytest.mark.skip(reason="result output for this is empty string")
def test_low_complication_product_yaml_is_valid(client):
    response = client.get("/products/ls5_fc_albers.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml')
    f.write(response.data)

    runner = CliRunner()

    result = runner.invoke(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name
        ]
    )

    assert result.output == 'Updated "ls5_fc_albers"\n'
    assert result.exit_code == 0


@pytest.mark.skip(reason="result output for this is empty string")
def test_simple_product_yaml_is_valid(client):
    response = client.get("/products/dsm1sv10.odc-product.yaml", follow_redirects=True)
    assert response.content_type == 'text/yaml'

    f = tempfile.NamedTemporaryFile(suffix='.yaml')
    f.write(response.data)

    runner = CliRunner()

    result = runner.invoke(
        datacube.scripts.cli_app.cli,
        [
            "product",
            "update",
            f.name
        ]
    )

    assert result.output == 'Updated "dsm1sv10"\n'
    assert result.exit_code == 0