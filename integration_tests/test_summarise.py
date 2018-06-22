import pytest
from datetime import datetime
from dateutil.tz import tzutc
from pathlib import Path

from cubedash._utils import default_utc
from cubedash.summary import TimePeriodOverview
from datacube.model import Range
from datacube.scripts.dataset import create_dataset, load_rules_from_types
from datacube.utils import read_documents

TEST_DATA_DIR = Path(__file__).parent / 'data'


def _populate_from_dump(session_dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = session_dea_index.products.get_by_name(expected_type)
    dataset_count = 0
    rules = load_rules_from_types(session_dea_index)
    for _, doc in read_documents(dump_path):
        created = session_dea_index.datasets.add(
            create_dataset(doc, None, rules),
        )

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(scope='module', autouse=True)
def populate_index(module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's session-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        module_dea_index,
        'ls8_nbar_scene',
        TEST_DATA_DIR / 'ls8-nbar-scene-sample-2017.yaml.gz'
    )
    _populate_from_dump(
        module_dea_index,
        'ls8_nbar_albers',
        TEST_DATA_DIR / 'ls8-nbar-albers-sample.yaml.gz'
    )
    return module_dea_index


def test_calc_month(summary_store):

    # One Month
    _expect_values(
        summary_store.calculate_summary(
            'ls8_nbar_scene',
            Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
        ),
        dataset_count=408,
        footprint_count=408,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0),
            end=datetime(2017, 5, 1, 0, 0)
        ),
        newest_creation_time=datetime(
            2017, 7, 4, 11, 18, 20, tzinfo=tzutc()
        ),
        timeline_period='day',
        timeline_count=30
    )


def test_calc_scene_year(summary_store):
    # One year, storing result.
    _expect_values(
        summary_store.update(
            'ls8_nbar_scene',
            year=2017,
            month=None,
            day=None,
        ),
        dataset_count=1789,
        footprint_count=1789,
        time_range=Range(
            begin=datetime(2017, 1, 1, 0, 0),
            end=datetime(2018, 1, 1, 0, 0)
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period='day',
        timeline_count=365
    )


def test_calc_scene_all_time(summary_store):
    # All time
    _expect_values(
        summary_store.update(
            'ls8_nbar_scene',
            year=None,
            month=None,
            day=None,
        ),
        dataset_count=3036,
        footprint_count=3036,
        time_range=Range(
            begin=datetime(2016, 1, 1, 0, 0),
            end=datetime(2018, 1, 1, 0, 0)
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period='month',
        timeline_count=24
    )


def test_calc_albers_summary_with_storage(summary_store):

    # Should not exist yet.
    summary = summary_store.get(
        'ls8_nbar_albers',
        year=None,
        month=None,
        day=None,
    )
    assert summary is None
    summary = summary_store.get(
        'ls8_nbar_albers',
        year=2017,
        month=None,
        day=None,
    )
    assert summary is None

    # Calculate overall summary
    summary = summary_store.get_or_update(
        'ls8_nbar_albers',
        year=2017,
        month=None,
        day=None,
    )
    _expect_values(
        summary,
        dataset_count=918,
        footprint_count=918,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0),
            end=datetime(2017, 6, 1, 0, 0)
        ),
        newest_creation_time=datetime(
            2017, 10, 25, 23, 9, 2, 486851, tzinfo=tzutc()
        ),
        timeline_period='day',
        # Data spans 61 days in 2017
        timeline_count=61
    )

    # get_or_update should now return the cached copy.
    cached_s = summary_store.get_or_update(
        'ls8_nbar_albers',
        year=2017,
        month=None,
        day=None,
    )
    assert cached_s.summary_gen_time is not None
    assert cached_s.summary_gen_time == summary.summary_gen_time, \
        "A new, rather than cached, summary was returned"
    assert cached_s.dataset_count == summary.dataset_count


def test_no_datasets_in_time(summary_store):
    # No datasets in 2018
    summary = summary_store.get_or_update(
        'ls8_nbar_albers',
        year=2018,
        month=None,
        day=None,
    )
    assert summary.dataset_count == 0, "There should be no datasets in 2018"


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

