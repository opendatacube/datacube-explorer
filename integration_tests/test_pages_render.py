"""
Test functions that iterate all pages to check that no error was
thrown during rendering.

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
from textwrap import indent
from typing import List

import pytest
from flask import Response
from flask.testing import FlaskClient

from cubedash._utils import alchemy_engine
from cubedash.summary import SummaryStore
from cubedash.summary._schema import CUBEDASH_SCHEMA
from cubedash.warmup import find_examples_of_all_public_urls
from datacube.index import Index


def test_all_pages_render(all_urls, client: FlaskClient):
    """Do all expected URLS render with HTTP OK response/"""
    assert_all_urls_render(all_urls, client)


def test_allows_null_product_fixed_fields(
    all_urls, client: FlaskClient, module_index: Index, summary_store: SummaryStore,
):
    """
    Pages should not fallover when fixed_metadata is null.

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
        alchemy_engine(module_index)
        .execute(f"update {CUBEDASH_SCHEMA}.product set fixed_metadata = null")
        .rowcount
    )
    assert update_count > 0, "There were no test products to update?"

    # THEN All pages should still render fine.
    assert_all_urls_render(all_urls, client)


def assert_all_urls_render(all_urls: List[str], client: FlaskClient):
    """Assert all given URLs return an OK HTTP response"""

    __tracebackhide__ = True

    for url in all_urls:
        response: Response = client.get(url, follow_redirects=True)

        if response.status_code != 200:
            max_failure_line_count = 5
            error_sample = "\n".join(
                response.data.decode("utf-8").split("\n")[:max_failure_line_count]
            )
            raise AssertionError(
                f"Response {response.status_code} from url f{url}. "
                f"Content:\n{indent(error_sample, ' ' * 4)}"
            )


@pytest.fixture()
def all_urls(summary_store: SummaryStore):
    return list(find_examples_of_all_public_urls(summary_store.index))
