"""
Load a lot of real-world DEA datasets (very slow)

And then check their statistics match expected.
"""
from datetime import datetime
from pathlib import Path

import pytest
from dateutil import tz
from dateutil.tz import tzutc

from cubedash.summary import SummaryStore
from datacube.index.hl import Doc2Dataset
from datacube.model import Range
from datacube.utils import read_documents

from .asserts import expect_values as _expect_values

TEST_DATA_DIR = Path(__file__).parent / "data"

DEFAULT_TZ = tz.gettz("Australia/Darwin")


def _populate_from_dump(session_dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = session_dea_index.products.get_by_name(expected_type)
    dataset_count = 0

    create_dataset = Doc2Dataset(session_dea_index)

    for _, doc in read_documents(dump_path):
        label = doc["ga_label"] if ("ga_label" in doc) else doc["id"]
        dataset, err = create_dataset(doc, f"file://example.com/test_dataset/{label}")
        assert dataset is not None, err
        created = session_dea_index.datasets.add(dataset)

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(scope="module", autouse=True)
def populate_index(module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        module_dea_index,
        "ls8_nbar_scene",
        TEST_DATA_DIR / "ls8-nbar-scene-sample-2017.yaml.gz",
    )
    _populate_from_dump(
        module_dea_index,
        "ls8_nbar_albers",
        TEST_DATA_DIR / "ls8-nbar-albers-sample.yaml.gz",
    )
    return module_dea_index


def test_generate_month(run_generate, summary_store: SummaryStore):
    run_generate("ls8_nbar_scene")
    # One Month
    _expect_values(
        summary_store.update("ls8_nbar_scene", 2017, 4, None),
        dataset_count=408,
        footprint_count=408,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2017, 5, 1, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2017, 7, 4, 11, 18, 20, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=30,
        crses={
            "EPSG:28355",
            "EPSG:28349",
            "EPSG:28352",
            "EPSG:28350",
            "EPSG:28351",
            "EPSG:28353",
            "EPSG:28356",
            "EPSG:28354",
        },
        size_bytes=245_344_352_585,
    )


def test_generate_scene_year(run_generate, summary_store: SummaryStore):
    run_generate()
    # One year
    _expect_values(
        summary_store.get("ls8_nbar_scene", year=2017, month=None, day=None),
        dataset_count=1792,
        footprint_count=1792,
        time_range=Range(
            begin=datetime(2017, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2018, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=365,
        crses={
            "EPSG:28355",
            "EPSG:28349",
            "EPSG:28352",
            "EPSG:28350",
            "EPSG:28351",
            "EPSG:28353",
            "EPSG:28356",
            "EPSG:28354",
        },
        size_bytes=1_060_669_242_142,
    )


def test_generate_scene_all_time(run_generate, summary_store: SummaryStore):
    run_generate("ls8_nbar_scene")

    # All time
    _expect_values(
        summary_store.get("ls8_nbar_scene", year=None, month=None, day=None),
        dataset_count=3036,
        footprint_count=3036,
        time_range=Range(
            begin=datetime(2016, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2018, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2018, 1, 10, 3, 11, 56, tzinfo=tzutc()),
        timeline_period="month",
        timeline_count=24,
        crses={
            "EPSG:28355",
            "EPSG:28349",
            "EPSG:28352",
            "EPSG:28357",
            "EPSG:28350",
            "EPSG:28351",
            "EPSG:28353",
            "EPSG:28356",
            "EPSG:28354",
        },
        size_bytes=1_805_759_242_975,
    )


def test_has_source_derived_product_links(run_generate, summary_store: SummaryStore):
    run_generate()

    albers = summary_store.get_product_summary("ls8_nbar_albers")
    scene = summary_store.get_product_summary("ls8_nbar_scene")
    telem = summary_store.get_product_summary("ls8_satellite_telemetry_data")

    print(repr([albers, scene, telem]))
    assert albers.source_products == ["ls8_nbar_scene"]
    assert albers.derived_products == []

    assert scene.source_products == ["ls8_level1_scene"]
    assert scene.derived_products == ["ls8_nbar_albers"]

    assert telem.source_products == []
    assert telem.derived_products == ["ls8_level1_scene"]


def test_generate_empty_time(run_generate, summary_store: SummaryStore):
    run_generate("ls8_nbar_albers")

    # No datasets in 2018
    summary = summary_store.get_or_update(
        "ls8_nbar_albers", year=2018, month=None, day=None
    )
    assert summary.dataset_count == 0, "There should be no datasets in 2018"
    # assert len(summary.timeline_dataset_counts) == 365, "Empty regions should still show up in timeline histogram"

    # Year that does not exist for LS8
    summary = summary_store.get("ls8_nbar_albers", year=2006, month=None, day=None)
    assert summary is None


def test_calc_empty(summary_store: SummaryStore):
    summary_store.refresh_all_products()

    # Should not exist.
    summary = summary_store.get("ls8_fake_product", year=2006, month=None, day=None)
    assert summary is None


def test_generate_day(run_generate, summary_store: SummaryStore):
    run_generate("ls8_nbar_albers")

    _expect_values(
        summary_store.get_or_update("ls8_nbar_albers", year=2017, month=5, day=2),
        dataset_count=29,
        footprint_count=29,
        time_range=Range(
            begin=datetime(2017, 5, 2, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2017, 5, 3, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2017, 10, 20, 8, 53, 26, 475_609, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=1,
        crses={"EPSG:3577"},
        size_bytes=None,
    )


def test_calc_albers_summary_with_storage(summary_store: SummaryStore):
    summary_store.refresh_all_products()

    # Should not exist yet.
    summary = summary_store.get("ls8_nbar_albers", year=None, month=None, day=None)
    assert summary is None
    summary = summary_store.get("ls8_nbar_albers", year=2017, month=None, day=None)
    assert summary is None

    # Calculate overall summary
    summary = summary_store.get_or_update(
        "ls8_nbar_albers", year=2017, month=None, day=None
    )
    _expect_values(
        summary,
        dataset_count=918,
        footprint_count=918,
        time_range=Range(
            begin=datetime(2017, 4, 1, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2017, 6, 1, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2017, 10, 25, 23, 9, 2, 486_851, tzinfo=tzutc()),
        timeline_period="day",
        # Data spans 61 days in 2017
        timeline_count=61,
        crses={"EPSG:3577"},
        # Ingested tiles don't store their size.
        # TODO: probably should represent this as None instead of zero?
        size_bytes=0,
    )

    # get_or_update should now return the cached copy.
    cached_s = summary_store.get_or_update(
        "ls8_nbar_albers", year=2017, month=None, day=None
    )
    assert cached_s.summary_gen_time is not None
    assert (
        cached_s.summary_gen_time == summary.summary_gen_time
    ), "A new, rather than cached, summary was returned"
    assert cached_s.dataset_count == summary.dataset_count
