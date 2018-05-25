from datetime import datetime
from dateutil.tz import tzutc

from pathlib import Path

from cubedash.summary import FileSummaryStore
from datacube.model import Range


def test_calc_month_scenes(populated_index, tmpdir):
    store = FileSummaryStore(populated_index, Path(tmpdir))

    s = store.calculate_summary(
        'ls8_nbar_scene',
        Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
    )
    print(repr(s))
    assert s.dataset_count == 208
    assert s.footprint_count == 208
    assert s.time_range == Range(
        begin=datetime(2017, 4, 1, 0, 0),
        end=datetime(2017, 5, 1, 0, 0)
    )
    assert s.newest_dataset_creation_time == datetime(
        2017, 7, 4, 11, 19, 23, tzinfo=tzutc()
    )
    assert len(s.dataset_counts) == 30

    # Test an update of all dates.
    # (note: this should be a separate test method, but we don't want to create
    # the populated_index multiple times. The fixture scope needs to be changed
    # upstream to allow it.)
    store = FileSummaryStore(populated_index, Path(tmpdir))

    s = store.update(
        'ls8_nbar_scene',
        year=None,
        month=None,
        day=None,
    )

    assert s.dataset_count == 2478
    assert s.footprint_count == 2478
    assert s.newest_dataset_creation_time == datetime(
        2018, 1, 10, 3, 11, 38, tzinfo=tzutc()
    )
    print(len(s.dataset_counts))
    assert len(s.dataset_counts) == 365
    # ??
    # assert s.period == 'day'
