from collections import Counter
from datetime import datetime, timedelta

import pytest
from dateutil import tz
from shapely import geometry as geo

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._stores import PgSummaryStore
from datacube.model import Range


def _overview():
    orig = TimePeriodOverview(
        1234,
        Counter(
            [
                datetime(2017, 1, 2, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 1, tzinfo=tz.tzutc()),
            ]
        ),
        {},
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
        footprint_count=0,
        newest_dataset_creation_time=datetime(2018, 1, 1, 1, 1, 1, tzinfo=tz.tzutc()),
        crses={"epsg:1234"},
    )
    return orig


def test_store_unchanged(summary_store: SummaryStore):
    """
    A put followed by a get should return identical data
    """
    orig = _overview()

    summary_store.get_last_updated()

    summary_store.put("some_product", 2017, None, None, orig)
    loaded = summary_store.get("some_product", 2017, None, None)

    # pytest has better error messages for dict comparison
    assert orig.__dict__ == loaded.__dict__
    assert orig == loaded


def test_store_empty(summary_store: SummaryStore):
    """
    Should be able to record a period with no datasets.
    """
    orig = TimePeriodOverview.empty()

    summary_store.put("some_product", 2017, 4, None, orig)
    loaded = summary_store.get("some_product", 2017, 4, None)

    # pytest has better error messages for dict comparison
    assert orig.__dict__ == loaded.__dict__
    assert orig == loaded


def test_get_null(summary_store: SummaryStore):
    """
    An area with nothing generated should come back as null.

    (It's important for us to distinguish between an area with zero datasets
    and an area where the summary/extent has not been generated.)
    """
    loaded = summary_store.get("some_product", 2019, 4, None)
    assert loaded is None


def test_store_updated(summary_store: SummaryStore):
    """
    We should be able to update summaries.
    """
    o = _overview()

    summary_store.put("some_product", 2017, None, None, o)
    loaded = summary_store.get("some_product", 2017, None, None)

    assert o is not loaded, (
        "Store should not return the original objects " "(they may change)"
    )

    o.dataset_count = 4321
    o.newest_dataset_creation_time = datetime(2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc())
    summary_store.put("some_product", 2017, None, None, o)

    loaded = summary_store.get("some_product", 2017, None, None)
    assert loaded.dataset_count == 4321
    assert loaded.newest_dataset_creation_time == datetime(
        2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc()
    )


def test_srid_lookup(summary_store: SummaryStore):
    if isinstance(summary_store, PgSummaryStore):
        srid = summary_store._target_srid()
        assert srid is not None
        assert isinstance(srid, int)

        srid2 = summary_store._target_srid()
        assert srid == srid2

        assert summary_store._get_srid_name(srid) == "EPSG:4326"

        # Cached?
        cache_hits = summary_store._get_srid_name.cache_info().hits
        assert summary_store._get_srid_name(srid) == "EPSG:4326"
        assert summary_store._get_srid_name.cache_info().hits > cache_hits


@pytest.mark.skip("Still relevant?")
def test_store_records_last_updated(summary_store: SummaryStore):
    o = _overview()
    o.summary_gen_time -= timedelta(hours=2)

    assert summary_store.get_last_updated() is None

    summary_store.put("some_product", 2017, None, None, o)
    summary_store.update(
        "some_product", None, None, None, generate_missing_children=False
    )
    summary_store.update(None, None, None, None, generate_missing_children=False)

    assert summary_store.get_last_updated() == o.summary_gen_time

    # Add another, so it's even newer...

    # A new one will be generated "now", so it will be newer than the above.
    summary_store.put("some_product", 2018, None, None, TimePeriodOverview.empty())
    summary_store.update(
        "some_product", None, None, None, generate_missing_children=False
    )
    summary_store.update(None, None, None, None, generate_missing_children=False)

    assert summary_store.get_last_updated() > o.summary_gen_time
