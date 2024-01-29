"""
Unit test for various app.config["CUBEDASH_DATA_S3_REGION"]
"""

import pytest
from flask import Flask, current_app

from cubedash._utils import as_external_url, as_resolved_remote_url


@pytest.fixture()
def app_s3_region_unset():
    app = Flask(__name__)
    return app


@pytest.fixture()
def app_s3_region_none():
    app = Flask(__name__)
    app.config["CUBEDASH_DATA_S3_REGION"] = None
    return app


@pytest.fixture()
def app_s3_region_string_none():
    app = Flask(__name__)
    app.config["CUBEDASH_DATA_S3_REGION"] = "None"
    app.config["SHOW_DATA_LOCATION"] = {"dea-public-data": "data.dea.ga.gov.au"}
    return app


@pytest.fixture()
def app_s3_region_empty_string():
    app = Flask(__name__)
    app.config["CUBEDASH_DATA_S3_REGION"] = ""
    return app


def test_as_external_url(app_s3_region_unset):
    with app_s3_region_unset.app_context():
        assert (
            as_external_url(
                "s3://some-data/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml",
                "",
            )
            == "s3://some-data/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml"
        )

        assert (
            as_external_url(
                "s3://some-data/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml",
                "None",
            )
            == "https://some-data.s3.None.amazonaws.com/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml"
        )

        assert (
            as_external_url(
                "s3://some-data/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml",
                None,
            )
            == "s3://some-data/L2/S2A_OPER_MSI_ARD__A030100_T56LNQ_N02.09/ARD-METADATA.yaml"
        )


def test_resolved_remote_url_s3_region_unset(app_s3_region_unset):
    with app_s3_region_unset.app_context():
        assert current_app.config.get("CUBEDASH_DATA_S3_REGION") is None

        assert (
            as_resolved_remote_url(None, "file://example.com/test_dataset/")
            == "file://example.com/test_dataset/"
        )

        assert (
            as_resolved_remote_url(None, "s3://example.com/test_dataset/")
            == "https://example.com.s3.ap-southeast-2.amazonaws.com/test_dataset/"
        )


def test_resolved_remote_url_none_s3_region(app_s3_region_none):
    with app_s3_region_none.app_context():
        assert current_app.config.get("CUBEDASH_DATA_S3_REGION") is None

        assert (
            as_resolved_remote_url(None, "file://example.com/test_dataset/")
            == "file://example.com/test_dataset/"
        )

        assert (
            as_resolved_remote_url(None, "s3://example.com/test_dataset/")
            == "s3://example.com/test_dataset/"
        )


def test_resolved_remote_url_string_none_s3_region(app_s3_region_string_none):
    with app_s3_region_string_none.app_context():
        assert current_app.config.get("CUBEDASH_DATA_S3_REGION") == "None"

        assert (
            as_resolved_remote_url(None, "file://example.com/test_dataset/")
            == "file://example.com/test_dataset/"
        )

        assert (
            as_resolved_remote_url(None, "s3://example.com/test_dataset/")
            == "https://example.com.s3.None.amazonaws.com/test_dataset/"
        )


def test_resolved_remote_url_empty_string_s3_region(app_s3_region_empty_string):
    with app_s3_region_empty_string.app_context():
        assert current_app.config.get("CUBEDASH_DATA_S3_REGION") == ""

        assert (
            as_resolved_remote_url(None, "file://example.com/test_dataset/")
            == "file://example.com/test_dataset/"
        )

        assert (
            as_resolved_remote_url(None, "s3://example.com/test_dataset/")
            == "s3://example.com/test_dataset/"
        )


def test_resolved_remote_url_data_browser(app_s3_region_string_none):
    with app_s3_region_string_none.app_context():
        assert current_app.config.get("SHOW_DATA_LOCATION") == {
            "dea-public-data": "data.dea.ga.gov.au"
        }

        assert (
            as_resolved_remote_url(
                None, "s3://dea-public-data/example/product/filepath"
            )
        ) == "https://data.dea.ga.gov.au/?prefix=example/product/"

        assert (
            as_resolved_remote_url(
                "s3://dea-public-data/example/product/filepath", "offset"
            )
        ) == "https://data.dea.ga.gov.au/example/product/offset"
