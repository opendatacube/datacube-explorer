
from flask.testing import FlaskClient
from io import StringIO
from pathlib import Path

from cubedash import _model
from ruamel.yaml import YAML
import yaml

from datacube.utils.serialise import SafeDatacubeDumper


from integration_tests.asserts import (
    check_dataset_count,
    get_html,
    get_text_response
)

from cubedash._utils import prepare_document_formatting
import pytest
import re


TEST_DATA_DIR = Path(__file__).parent / "data"

@pytest.mark.skip(reason="loading back to yaml will convert it to float")
def test_s2a_ard_nbar_yaml(client: FlaskClient):
    doc, _ = get_text_response(client, 'products/ga_s2a_ard_nbar_granule.odc-product.yaml')


    doc_yaml = YAML(typ="safe", pure=True).load(StringIO(doc))

    for m in doc_yaml['measurements']:
        if m["name"] == 'nbar_swir_2':
            assert str(m['spectral_definition']['response'][0]) == '7.0e-06'
            assert m['spectral_definition']['response'][1] == '7.0e-06'
            assert m['spectral_definition']['response'][2] == '7.0e-06'



@pytest.mark.skip()
def test_s2a_ard_nbar_page(client: FlaskClient):
    html = get_html(client, 'products/ga_s2a_ard_nbar_granule')

    spectral_response = html.find("#raw-doc #nbar_swir_2 #spectral_definition", first=True)
    assert spectral_response.find(".array-item")[0].find(".value", first=True).text ==  '7.0e-06'
    assert spectral_response.find(".array-item")[1].find(".value", first=True).text ==  '7.0e-06'
    assert spectral_response.find(".array-item")[2].find(".value", first=True).text ==  '7.0e-06'