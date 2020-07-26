# Test Sentry integration
import pytest
import cubedash
from integration_tests.asserts import get_html

@pytest.fixture
def sentry_client(empty_client : FlaskClient) -> FlaskClient:
    cubedash.app.config["SENTRY_CONFIG"] = {
        'dsn': '___DSN___',
        'include_paths': ['cubedash'],
    }
    return empty_client

def test_sentry(sentry_client : FlaskClient):
    html = get_html(sentry_client, "/")
    assert html
