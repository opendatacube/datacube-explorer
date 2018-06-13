from datetime import datetime
from dateutil import tz

from cubedash.summary import TimePeriodOverview, FileSummaryStore
from datacube.model import Range


def test_store(session_dea_index, tmppath):
    orig = TimePeriodOverview(
        1234,
        None,
        {},
        timeline_period='day',
        time_range=Range(datetime(2017, 1, 2), datetime(2017, 2, 3)),
        footprint_geometry=None,
        footprint_count=0,
        newest_dataset_creation_time=datetime(2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc())
    )

    store = FileSummaryStore(session_dea_index, tmppath)

    store.put('some_product', 2017, None, None, orig)
    loaded = store.get('some_product', 2017, None, None)

    assert orig == loaded

