from textwrap import indent
from typing import List

import pytest
from datacube import Datacube
from flask import Response
from flask.testing import FlaskClient

from cubedash import _utils
from cubedash.summary import SummaryStore, _schema

METADATA_TYPES = [
    "metadata/landsat_l1_scene.yaml",
]
PRODUCTS = [
    "products/dsm1sv10.odc-product.yaml",
    "products/wofs_albers.yaml",
    "products/ls7_scenes.odc-product.yaml",
    "products/ls7_nbart_albers.odc-product.yaml",
    "products/ls8_scenes.odc-product.yaml",
    "products/ls8_nbart_albers.odc-product.yaml",
]
DATASETS = [
    "datasets/wofs-albers-sample.yaml.gz",
]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


def assert_all_urls_render(all_urls: List[str], client: FlaskClient):
    """Assert all given URLs return an OK HTTP response

    These can be used to test that the application works when
    the DB/Index is put in an unusual state -- the most common
    situation by far is a hard rendering error.

    *caution*

    Please don't rely on this for increasing test coverage of
    normal functionality.

    Other test modules, such as `test_page_loads.py`, are more
    useful to imitate for normal testing as they test the
    actual values on the page.

    This does catch a lot of bugs though, at low effort.
    """

    __tracebackhide__ = True

    for url in all_urls:
        try:
            response: Response = client.get(url, follow_redirects=True)
        except Exception as e:
            raise AssertionError(f"Error rendering url f{url}.") from e

        if response.status_code != 200:
            max_failure_line_count = 5
            error_sample = "\n".join(
                response.data.decode("utf-8").split("\n")[:max_failure_line_count]
            )
            raise AssertionError(
                f"Response {response.status_code} from url '{url}'. "
                f"Content:\n{indent(error_sample, ' ' * 4)}"
            )


def test_all_pages_render(all_urls, client: FlaskClient):
    """
    Do all expected URLS render with HTTP OK response with our normal test data?
    """
    assert_all_urls_render(all_urls, client)


def test_allows_null_product_fixed_fields(
    all_urls,
    client: FlaskClient,
    odc_test_db: Datacube,
    summary_store: SummaryStore,
):
    """
    Pages should not fall over when fixed_metadata is null.

    Older versions of cubedash-gen don't write the fixed_metadata column, so
    it can be null in legacy and migrated deployments.

    (and null is desired behaviour here: null indicates "not known",
    while "empty dict" indicates there are zero fields of metadata)
    """

    # WHEN we have some products summarised
    assert (
        summary_store.list_complete_products()
    ), "There's no summarised products to test"

    # AND there's some with null fixed_metadata (ie. pre-Explorer0-EO3-update)
    update_count = (
        _utils.alchemy_engine(odc_test_db.index)
        .execute(f"update {_schema.PRODUCT.fullname} set fixed_metadata = null")
        .rowcount
    )
    assert update_count > 0, "There were no test products to update?"

    # THEN All pages should still render fine.
    assert_all_urls_render(all_urls, client)
