import pytest

from flask.testing import FlaskClient
import cubedash

from integration_tests.asserts import (
    get_html
)


@pytest.fixture()
def app_configured_client(client: FlaskClient):
    cubedash.app.config["CUBEDASH_INSTANCE_TITLE"] = "Development - ODC"
    cubedash.app.config["CUBEDASH_SISTER_SITES"] = (
        ('Production - ODC', 'http://prod.odc.example'),
        ('Production - NCI', 'http://nci.odc.example'),
    )
    return cubedash.app.test_client()


def test_instance_title(app_configured_client: FlaskClient):
    html = get_html(app_configured_client, "/about")

    instance_title = html.find(
        ".instance-title",
        first=True
    ).text
    assert instance_title == 'Development - ODC'


def test_sister_sites(app_configured_client: FlaskClient):
    html = get_html(app_configured_client, "/about")

    sister_instances = html.find(
        "#sister-site-menu ul li"
    )
    assert len(sister_instances) == 2
