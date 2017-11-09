from datetime import datetime

from werkzeug.datastructures import MultiDict

from cubedash._utils import DEFAULT_PLATFORM_END_DATE, query_to_search
from datacube.index._api import Index
from datacube.model import Range


def test_parse_query_args(dea_index: Index):
    """
    A user gives time start/end: they should be parsed as a single time field,
    and restricted to the current product.
    """

    product = dea_index.products.get_by_name("ls7_level1_scene")
    res = query_to_search(
        MultiDict((("time-begin", "2017-08-08"), ("time-end", "2017-08-09"))), product
    )

    assert res == dict(
        time=Range(datetime(2017, 8, 8), datetime(2017, 8, 9)), product=product.name
    )


def test_default_args(dea_index: Index):
    """
    When the user provides no search args we should constraint their query

    (this is the default when the page loads with no search parameters.)
    """
    # Sanity check: we assume this value below.
    assert DEFAULT_PLATFORM_END_DATE["LANDSAT_5"] == datetime(2011, 11, 30)

    product = dea_index.products.get_by_name("ls5_level1_scene")

    # No args. In should constrain it to the latest month for the satellite/product.
    res = query_to_search(MultiDict(()), product)

    # The last month of LANDSAT_5 for this product
    assert res == dict(
        time=Range(datetime(2011, 10, 30), datetime(2011, 11, 30)), product=product.name
    )
