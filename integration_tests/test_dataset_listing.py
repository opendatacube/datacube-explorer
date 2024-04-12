from datetime import datetime

import pytest
from datacube import Datacube
from datacube.index import Index
from datacube.model import Range
from werkzeug.datastructures import MultiDict

from cubedash._utils import DEFAULT_PLATFORM_END_DATE, query_to_search

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


def test_parse_query_args(odc_test_db: Datacube):
    """
    A user gives time start/end: they should be parsed as a single time field,
    and restricted to the current product.
    """

    product = odc_test_db.index.products.get_by_name("ls7_level1_scene")
    res = query_to_search(
        MultiDict(
            (
                ("time-begin", "2017-08-08"),
                ("time-end", "2017-08-09"),
                # If they specify their range backwards (high, low), we should parse it reversed.
                # (Intention is "Numbers are between these values")
                ("gqa-begin", 3),
                ("gqa-end", -3),
            )
        ),
        product,
    )

    assert res == dict(
        time=Range(datetime(2017, 8, 8), datetime(2017, 8, 9)), gqa=Range(-3, 3)
    )


@pytest.mark.skip(
    reason="Should be updated to do a flask request. Default params are there."
)
def test_default_args(dea_index: Index):
    """
    When the user provides no search args we should constraint their query

    (this is the default when the page loads with no search parameters.)
    """
    # Sanity check: we assume this value below.
    assert DEFAULT_PLATFORM_END_DATE["LANDSAT_5"] == datetime(2011, 11, 30)

    product = dea_index.products.get_by_name("ls5_level1_scene")

    res = query_to_search(MultiDict(()), product)

    # The last month of LANDSAT_5 for this product
    assert (
        res
        == dict(
            # time=Range(datetime(2011, 10, 30), datetime(2011, 11, 30)),
            # product=product.name
        )
    )
