import time
from collections import Counter
from datetime import datetime

import pytest
from dateutil import tz
from shapely import geometry as geo

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._stores import ProductSummary
from cubedash.summary._summarise import Summariser
from datacube.model import Range


def _overview():
    orig = TimePeriodOverview(
        dataset_count=4,
        timeline_dataset_counts=Counter(
            [
                datetime(2017, 1, 2, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 1, tzinfo=tz.tzutc()),
            ]
        ),
        region_dataset_counts=Counter(["1_2", "1_2", "3_4", "4_5"]),
        timeline_period="day",
        time_range=Range(
            datetime(2017, 1, 2, tzinfo=tz.tzutc()),
            datetime(2017, 2, 3, tzinfo=tz.tzutc()),
        ),
        footprint_geometry=geo.Polygon(
            [
                # ll:
                (-29.882_024, 113.105_949),
                # lr:
                (-29.930_607, 115.464_187),
                # ur:
                (-27.849_244, 115.494_523),
                # ul
                (-27.804_641, 113.18267),
            ]
        ),
        footprint_crs="EPSG:3577",
        footprint_count=3,
        newest_dataset_creation_time=datetime(2018, 1, 1, 1, 1, 1, tzinfo=tz.tzutc()),
        crses={"epsg:1234"},
        size_bytes=123_400_000,
    )
    return orig


def test_add_period_list():
    total = TimePeriodOverview.add_periods([])
    assert total.dataset_count == 0

    joined = TimePeriodOverview.add_periods([_overview(), _overview(), total])
    assert joined.dataset_count == _overview().dataset_count * 2
    assert _overview().footprint_geometry.area == pytest.approx(
        joined.footprint_geometry.area
    )

    assert sum(joined.region_dataset_counts.values()) == joined.dataset_count
    assert sum(joined.timeline_dataset_counts.values()) == joined.dataset_count

    assert joined.crses == _overview().crses
    assert joined.size_bytes == _overview().size_bytes * 2

    assert sorted(joined.region_dataset_counts.keys()) == ["1_2", "3_4", "4_5"]


def test_add_no_periods(summary_store: SummaryStore):
    """
    All the get/update methods should work on products with no datasets.
    """
    summary_store._set_product_extent(
        ProductSummary("test_empty_product", 0, None, None)
    )
    summary_store.get_or_update("test_empty_product", 2015, 7, 4)
    summary_store.get_or_update("test_empty_product", 2015, 7, None)
    summary_store.get_or_update("test_empty_product", 2015, None, None)
    summary_store.get_or_update("test_empty_product", None, None, None)


def test_get_null(summary_store: SummaryStore):
    """
    An area with nothing generated should come back as null.

    (It's important for us to distinguish between an area with zero datasets
    and an area where the summary/extent has not been generated.)
    """
    loaded = summary_store.get("some_product", 2019, 4, None)
    assert loaded is None


def test_srid_lookup(summariser: Summariser):
    srid = summariser._target_srid()
    assert srid is not None
    assert isinstance(srid, int)

    srid2 = summariser._target_srid()
    assert srid == srid2

    assert summariser._get_srid_name(srid) == "EPSG:3577"

    # Cached?
    cache_hits = summariser._get_srid_name.cache_info().hits
    assert summariser._get_srid_name(srid) == "EPSG:3577"
    assert summariser._get_srid_name.cache_info().hits > cache_hits


def test_put_get_summaries(summary_store: SummaryStore):
    """
    Test the serialisation/deserialisation from postgres
    """
    o = _overview()
    assert o.summary_gen_time is None, "Generation time should be set by server"

    product_name = "some_product"
    summary_store._set_product_extent(
        ProductSummary(product_name, 4321, datetime(2017, 1, 1), datetime(2017, 4, 1))
    )

    summary_store._put(product_name, 2017, None, None, o)
    loaded = summary_store.get(product_name, 2017, None, None)

    assert o is not loaded, (
        "Store should not return the original objects " "(they may change)"
    )
    assert (
        o.summary_gen_time is not None
    ), "Summary-gen-time should have been added by the server"
    original_gen_time = o.summary_gen_time

    assert o.footprint_geometry.area == pytest.approx(4.857_924_619_872)

    assert loaded.dataset_count == 4
    assert (
        sum(loaded.region_dataset_counts.values()) == 4
    ), "Region dataset counts don't match total count"
    assert sorted(loaded.region_dataset_counts.keys()) == [
        "1_2",
        "3_4",
        "4_5",
    ], "Incorrect set of regions"
    assert o.footprint_crs == loaded.footprint_crs
    assert loaded.footprint_crs == "EPSG:3577"
    assert loaded.footprint_srid == 3577
    assert loaded.footprint_geometry.area == pytest.approx(o.footprint_geometry.area)

    o.dataset_count = 4321
    o.newest_dataset_creation_time = datetime(2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc())
    time.sleep(1)
    summary_store._put(product_name, 2017, None, None, o)
    assert o.summary_gen_time != original_gen_time

    loaded = summary_store.get(product_name, 2017, None, None)
    assert loaded.dataset_count == 4321
    assert loaded.newest_dataset_creation_time == datetime(
        2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc()
    )
    assert (
        loaded.summary_gen_time != original_gen_time
    ), "An update should update the generation time"


def test_generate_empty(run_generate):
    """
    Run cubedash.generate on a cube with no datasets.

    Proper tests of 'generate' are in test_summarise_data.py, but take much longer to run.
    This catches many simple DB, product and config setup issues quickly.
    """
    run_generate()


def test_generate_raises_error(run_generate):
    """
    generate should return an error when an unknown product is asked for explicitly.
    """
    result = run_generate("fake_product", expect_success=False)
    assert result.exit_code != 0, (
        f"Command should return an error when unknown products are specified. "
        f"Output: {result.output}"
    )
