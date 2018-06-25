from collections import Counter
from datetime import datetime
from dateutil import tz

from cubedash.summary import TimePeriodOverview
from datacube.model import Range
from shapely import geometry as geo


def test_store(summary_store):
    orig = TimePeriodOverview(
        1234,
        Counter([
            datetime(2017, 1, 2, tzinfo=tz.tzutc()),
            datetime(2017, 1, 3, tzinfo=tz.tzutc()),
            datetime(2017, 1, 3, tzinfo=tz.tzutc()),
            datetime(2017, 1, 1, tzinfo=tz.tzutc())]),
        {},
        timeline_period='day',
        time_range=Range(
            datetime(2017, 1, 2, tzinfo=tz.tzutc()),
            datetime(2017, 2, 3, tzinfo=tz.tzutc())
        ),
        footprint_geometry=geo.Polygon([
            # ll:
            (-29.882024, 113.105949),
            # lr:
            (-29.930607, 115.464187),
            # ur:
            (-27.849244, 115.494523),
            # ul
            (-27.804641, 113.18267),
        ]),
        footprint_count=0,
        newest_dataset_creation_time=datetime(2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc())
    )

    summary_store.put('some_product', 2017, None, None, orig)
    loaded = summary_store.get('some_product', 2017, None, None)

    # pytest has better error messages for dict comparison
    assert orig.__dict__ == loaded.__dict__
    assert orig == loaded


def test_store_empty(summary_store):
    # A period with no datasets.
    orig = TimePeriodOverview(0, None, None, None, None, None, None, None)

    summary_store.put('some_product', 2017, 4, None, orig)
    loaded = summary_store.get('some_product', 2017, 4, None)

    # pytest has better error messages for dict comparison
    assert orig.__dict__ == loaded.__dict__
    assert orig == loaded
