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

        assert response.status_code == 200, (
            f"Response {response.status_code} from url f{url}. "
            f"Content:\n{indent(response.data.decode('utf-8'), ' ' * 4)}"
        )


@pytest.fixture()
def all_urls(summary_store: SummaryStore):
    return list(_find_all_public_urls(summary_store.index))


def _find_all_public_urls(index: Index):
    yield "/"
    yield "/about"
    yield "/products.txt"
    yield "/product-audit"
    yield "/product-audit/day-times.txt"

    for mdt in index.metadata_types.get_all():
        name = mdt.name
        yield f"/metadata-type/{name}"
        # yield f"/metadata-type/{name}.odc-type.yaml"

    for dt in index.products.get_all():
        name = dt.name
        yield f"/{name}"
        yield f"/datasets/{name}"
        yield f"/product/{name}"
        # yield f"/product/{name}.odc-product.yaml"

        has_datasets = index.datasets.search_eager(product=name, limit=1)
        if has_datasets:
            dataset = has_datasets[0]
            time = dataset.center_time
            yield f"/{name}/{time:%Y}"
            yield f"/{name}/{time:%Y%/m}"
            yield f"/{name}/{time::%Y/%m/%d}"
            yield f"/datasets/{name}/{time:%Y}"
            yield f"/datasets/{name}/{time:%Y%/m}"
            yield f"/datasets/{name}/{time::%Y/%m/%d}"

            yield f"/api/datasets/{name}"
            yield f"/api/regions/{name}/{time::%Y/%m/%d}"
            yield f"/api/footprint/{name}/{time::%Y/%m/%d}"

            # TODO: Do non-region_code regions too (such as ingested data)
            # TODO: Actually we have no EO3 in this test data, so it does nothing.
            #       Maybe add test data from test_eo3_support.py?
            if "region_code" in dataset.metadata.fields:
                yield f"/region/{dataset.metadata.region_code}"
                yield f"/region/{dataset.metadata.region_code}/{time::%Y/%m/%d}"

    for [dataset_id] in index.datasets.search_returning(("id",), limit=10):
        yield f"/dataset/{dataset_id}"
        # yield f"/dataset/{dataset_id}.odc-metadata.yaml"
