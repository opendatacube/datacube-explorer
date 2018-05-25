from datetime import datetime
from pathlib import Path

import pytest
from dateutil.tz import tzutc

from cubedash.summary import FileSummaryStore
from datacube.model import Range


def test_calc_scene_summary(populated_scene_index, tmpdir):
    store = FileSummaryStore(populated_scene_index, Path(tmpdir))

    s = store.calculate_summary(
        "ls8_nbar_scene", Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
    )
    print(repr(s))
    assert s.dataset_count == 208
    assert s.footprint_count == 208
    assert s.time_range == Range(
        begin=datetime(2017, 4, 1, 0, 0), end=datetime(2017, 5, 1, 0, 0)
    )
    assert s.newest_dataset_creation_time == datetime(
        2017, 7, 4, 11, 19, 23, tzinfo=tzutc()
    )
    assert len(s.dataset_counts) == 30

    # Test an update of all dates.
    # (note: this should be a separate test method, but we don't want to create
    # the populated_index multiple times. The fixture scope needs to be changed
    # upstream to allow it.)
    s = store.update("ls8_nbar_scene", year=None, month=None, day=None)

    print(repr(s))
    assert s.dataset_count == 2478
    assert s.footprint_count == 2478
    assert s.newest_dataset_creation_time == datetime(
        2018, 1, 10, 3, 11, 38, tzinfo=tzutc()
    )
    assert len(s.dataset_counts) == 365
    # ??
    # assert s.period == 'day'


def test_calc_albers_summary(populated_albers_index, tmpdir):
    store = FileSummaryStore(populated_albers_index, Path(tmpdir))

    # Should not exist yet.
    summary = store.get("ls8_nbar_scene", year=None, month=None, day=None)
    assert summary is None
    summary = store.get("ls8_nbar_scene", year=2017, month=None, day=None)
    assert summary is None

    # Calculate overall summary
    summary = store.get_or_update("ls8_nbar_scene", year=2017, month=None, day=None)
    print(repr(summary))
    assert summary.dataset_count == 617
    assert summary.footprint_count == 617
    assert summary.newest_dataset_creation_time == datetime(
        2017, 7, 11, 4, 32, 11, tzinfo=tzutc()
    )
    # Data spans 61 days in 2017
    assert len(summary.dataset_counts) == 61

    # get_or_update should now return the cached copy.
    cached_s = store.get_or_update("ls8_nbar_scene", year=2017, month=None, day=None)
    assert cached_s.summary_gen_time is not None
    assert cached_s.summary_gen_time == summary.summary_gen_time
    assert cached_s.dataset_count == summary.dataset_count

    # No datasets in 2018
    summary = store.get_or_update("ls8_nbar_scene", year=2018, month=None, day=None)
    assert summary.dataset_count == 0, "There should be no datasets in 2018"
