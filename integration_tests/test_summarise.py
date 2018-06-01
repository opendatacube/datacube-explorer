import pytest
from datetime import datetime
from dateutil.tz import tzutc

from pathlib import Path

from cubedash._utils import default_utc
from cubedash.summary import FileSummaryStore, TimePeriodOverview
from datacube.model import Range


def test_calc_scene_summary(populated_scene_index, tmpdir):
    store = FileSummaryStore(populated_scene_index, Path(tmpdir))

    # One Month
    _expect_values(
        store.calculate_summary(
            'ls8_nbar_scene',
            Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
        ),
        dataset_count=99,
        footprint_count=99,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0),
            end=datetime(2017, 5, 1, 0, 0)
        ),
        newest_creation_time=datetime(
            2017, 7, 4, 11, 17, 6, tzinfo=tzutc()
        ),
        timeline_period='day',
        timeline_count=30
    )

    # One year, storing result.
    _expect_values(
        store.update(
            'ls8_nbar_scene',
            year=2017,
            month=None,
            day=None,
        ),
        dataset_count=1227,
        footprint_count=1227,
        time_range=Range(
            begin=datetime(2017, 1, 1, 0, 0),
            end=datetime(2018, 1, 1, 0, 0)
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period='day',
        timeline_count=365
    )

    # All time
    _expect_values(
        store.update(
            'ls8_nbar_scene',
            year=None,
            month=None,
            day=None,
        ),
        dataset_count=2474,
        footprint_count=2474,
        time_range=Range(
            begin=datetime(2016, 1, 1, 0, 0),
            end=datetime(2018, 1, 1, 0, 0)
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period='month',
        timeline_count=24
    )


def _expect_values(s: TimePeriodOverview,
                   dataset_count: int,
                   footprint_count: int,
                   time_range: Range,
                   newest_creation_time: datetime,
                   timeline_period: str,
                   timeline_count: int):
    __tracebackhide__ = True

    try:
        assert s.dataset_count == dataset_count, "wrong dataset count"
        assert s.footprint_count == footprint_count, "wrong footprint count"
        assert s.time_range == time_range, "wrong dataset time range"
        assert s.newest_dataset_creation_time == default_utc(
            newest_creation_time
        ), "wrong newest dataset creation"
        assert s.timeline_period == timeline_period, (
            f"Should be a {timeline_period}, "
            f"not {s.timeline_period} timeline"
        )
        assert len(s.timeline_dataset_counts) == timeline_count, (
            "wrong timeline entry count"
        )
        assert sum(s.timeline_dataset_counts.values()) == s.dataset_count, (
            "timeline count doesn't match dataset count"
        )

        assert s.summary_gen_time is not None, (
            "Missing summary_gen_time (there's a default)"
        )

    except AssertionError:
        print(f"""Got:
        dataset_count {s.dataset_count}
        footprint_count {s.footprint_count}
        time range: {s.time_range}
        newest: {repr(s.newest_dataset_creation_time)}
        timeline
            period: {s.timeline_period}
            dataset_counts: {len(s.timeline_dataset_counts)}
        """)
        raise


def test_calc_albers_summary(populated_albers_index, tmpdir):
    store = FileSummaryStore(populated_albers_index, Path(tmpdir))

    # Should not exist yet.
    summary = store.get(
        'ls8_nbar_scene',
        year=None,
        month=None,
        day=None,
    )
    assert summary is None
    summary = store.get(
        'ls8_nbar_scene',
        year=2017,
        month=None,
        day=None,
    )
    assert summary is None

    # Calculate overall summary
    summary = store.get_or_update(
        'ls8_nbar_scene',
        year=2017,
        month=None,
        day=None,
    )
    _expect_values(
        summary,
        dataset_count=617,
        footprint_count=617,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0),
            end=datetime(2017, 6, 1, 0, 0)
        ),
        newest_creation_time=datetime(
            2017, 7, 11, 4, 32, 11, tzinfo=tzutc()
        ),
        timeline_period='day',
        # Data spans 61 days in 2017
        timeline_count=61
    )

    # get_or_update should now return the cached copy.
    cached_s = store.get_or_update(
        'ls8_nbar_scene',
        year=2017,
        month=None,
        day=None,
    )
    assert cached_s.summary_gen_time is not None
    assert cached_s.summary_gen_time == summary.summary_gen_time, \
        "A new, rather than cached, summary was returned"
    assert cached_s.dataset_count == summary.dataset_count

    # No datasets in 2018
    summary = store.get_or_update(
        'ls8_nbar_scene',
        year=2018,
        month=None,
        day=None,
    )
    assert summary.dataset_count == 0, "There should be no datasets in 2018"
