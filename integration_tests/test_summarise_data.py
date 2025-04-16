"""
Load a lot of real-world DEA datasets (very slow)

And then check their statistics match expected.
"""

from datetime import datetime, timedelta
from uuid import UUID

import pytest
from datacube import Datacube
from datacube.index import Index
from datacube.model import Product, Range
from dateutil import tz
from dateutil.tz import tzutc
from sqlalchemy import text

from cubedash.summary import SummaryStore
from cubedash.summary._extents import GridRegionInfo
from cubedash.summary._schema import CUBEDASH_SCHEMA

from .asserts import expect_values as _expect_values

DEFAULT_TZ = tz.gettz("Australia/Darwin")

METADATA_TYPES = [
    "metadata/eo3_landsat_ard.odc-type.yaml",
    "metadata/landsat_l1_scene.yaml",
]
PRODUCTS = [
    "products/ga_ls8c_ard_3.odc-product.yaml",
    "products/ga_ls9c_ard_3.odc-product.yaml",
    "products/ga_ls_fc_3.odc-product.yaml",
    "products/ga_ls_fc_pc_cyear_3.odc-product.yaml",
    "products/ls8_nbar_albers.odc-product.yaml",
    "products/ls8_scenes.odc-product.yaml",
]
DATASETS = [
    "datasets/ga_ls8c_ard_3-sample.yaml",
    "datasets/ga_ls9c_ard_3-sample.yaml",
    "datasets/ga_ls_fc_3-sample.yaml",
    "datasets/ga_ls_fc_pc_cyear_3-sample.yaml",
    "datasets/ls8-nbar-scene-sample-2017.yaml.gz",
    "datasets/ls8-nbar-albers-sample.yaml.gz",
]


# Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_generate_month(run_generate, summary_store: SummaryStore) -> None:
    run_generate("ls8_nbar_scene")
    # One Month
    _expect_values(
        summary_store.get("ls8_nbar_scene", 2017, 4, None),
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


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_generate_scene_year(run_generate, summary_store: SummaryStore) -> None:
    run_generate(multi_processed=True)
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


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_generate_scene_all_time(run_generate, summary_store: SummaryStore) -> None:
    run_generate("ls8_nbar_scene")

    # All time
    summary = summary_store.get("ls8_nbar_scene", year=None, month=None, day=None)
    assert (
        summary_store.index.datasets.count(product="ls8_nbar_scene")
        == summary.dataset_count
    )
    _expect_values(
        summary,
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


def test_generate_incremental_archivals(
    run_generate, summary_store: SummaryStore
) -> None:
    run_generate("ga_ls9c_ard_3")
    index = summary_store.index

    # When we have a summarised product...
    original_summary = summary_store.get("ga_ls9c_ard_3")
    original_dataset_count = original_summary.dataset_count

    # ... and we archive one dataset ...
    product_name = "ga_ls9c_ard_3"
    dataset_id = _one_dataset(index, product_name)
    try:
        index.datasets.archive([dataset_id])

        # ... the next generation should catch it and update with one less dataset....
        run_generate("ga_ls9c_ard_3")
        assert (
            summary_store.get("ga_ls9c_ard_3").dataset_count
            == original_dataset_count - 1
        ), "Expected dataset count to decrease after archival"
    finally:
        # Now let's restore the dataset!
        index.datasets.restore([dataset_id])

    # It should be in the count again.
    # (this change should work because the new 'updated' column will be bumped on restore)
    run_generate("ga_ls9c_ard_3")
    assert summary_store.get("ga_ls9c_ard_3").dataset_count == original_dataset_count, (
        "A dataset that was restored from archival was not refreshed by Explorer"
    )


def _one_dataset(index: Index, product_name: str):
    [[dataset_id]] = index.datasets.search_returning(
        ("id",), product=product_name, limit=1
    )
    return dataset_id


def test_dataset_changing_product(run_generate, summary_store: SummaryStore) -> None:
    """
    If a dataset it updated to be in a different product, Explorer should
    correctly update its summaries.

    (this really happened at NCI previously)

    This is a trickier case than regular updates because everything in Explorer
    is product-specific. Summarising one product at a time, etc.
    """
    run_generate("ga_ls9c_ard_3")
    index = summary_store.index

    dataset_id = _one_dataset(index, "ga_ls9c_ard_3")
    our_product = summary_store.get_product("ga_ls9c_ard_3")
    other_product = summary_store.get_product("ga_ls8c_ard_3")

    # When we have a summarised product...
    original_summary = summary_store.get("ga_ls9c_ard_3")
    original_dataset_count = original_summary.dataset_count

    try:
        # Move the dataset to another product
        _change_dataset_product(index, dataset_id, other_product)
        assert index.datasets.get(dataset_id).product.name == "ga_ls8c_ard_3"

        # Explorer should remove it too.
        print(f"Test dataset: {dataset_id}")
        # TODO: Make this work without a force-refresh.
        #       It's hard because we're scanning for updated datasets in the product...
        #       but it's not in the product. And the incremental updater misses it.
        #       So we have to force the non-incremental updater.
        run_generate("ga_ls8c_ard_3", "ga_ls9c_ard_3", "--force-refresh")

        assert (
            summary_store.get("ga_ls9c_ard_3").dataset_count
            == original_dataset_count - 1
        ), "Expected dataset to be removed after product change"

    finally:
        # Now change it back
        _change_dataset_product(index, dataset_id, our_product)

    run_generate("ga_ls8c_ard_3", "ga_ls9c_ard_3", "--force-refresh")
    assert summary_store.get("ga_ls9c_ard_3").dataset_count == original_dataset_count, (
        "Expected dataset to be added again after the product changed back"
    )


def _change_dataset_product(
    index: Index, dataset_id: UUID, other_product: Product
) -> None:
    if index.name == "pg_index":
        table_name = "agdc.dataset"
        ref_column = "dataset_type_ref"
    else:
        table_name = "odc.dataset"
        ref_column = "product_ref"
    with index._db._engine.begin() as conn:
        rows_changed = conn.execute(
            text(
                f"update {table_name} set {ref_column}=:product_id where id=:dataset_id"
            ),
            {
                "product_id": other_product.id,
                "dataset_id": dataset_id,
            },
        ).rowcount
    assert rows_changed == 1


def test_location_sampling(run_generate, summary_store: SummaryStore) -> None:
    location_samples = summary_store.product_location_samples("ga_ls8c_ard_3")
    assert len(location_samples) == 1

    [sample] = location_samples
    assert sample.uri_scheme == "file"
    assert sample.common_prefix == "file://example.com/test_dataset/"
    assert len(sample.example_uris) == 3
    assert all(
        uri.startswith("file://example.com/test_dataset/")
        for uri in sample.example_uris
    )


def test_has_source_derived_product_links(
    run_generate, summary_store: SummaryStore
) -> None:
    run_generate()

    ls_fc_pc = summary_store.get_product_summary("ga_ls_fc_pc_cyear_3")
    ls_fc = summary_store.get_product_summary("ga_ls_fc_3")
    ls8_ard = summary_store.get_product_summary("ga_ls8c_ard_3")

    print(repr([ls_fc_pc, ls_fc, ls8_ard]))
    assert ls_fc_pc.source_products == ["ga_ls_fc_3"]
    assert ls_fc_pc.derived_products == []

    assert ls_fc.source_products == ["ga_ls8c_ard_3"]
    assert ls_fc.derived_products == ["ga_ls_fc_pc_cyear_3"]

    assert ls8_ard.source_products == []
    assert ls8_ard.derived_products == ["ga_ls_fc_3"]


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_product_fixed_fields(run_generate, summary_store: SummaryStore) -> None:
    run_generate()

    albers = summary_store.get_product_summary("ls8_nbar_albers")
    scene = summary_store.get_product_summary("ls8_nbar_scene")
    telem = summary_store.get_product_summary("ls8_satellite_telemetry_data")

    assert scene.fixed_metadata == {
        "platform": "LANDSAT_8",
        "instrument": "OLI_TIRS",
        "product_type": "nbar",
        "format": "GeoTIFF",
        "gsi": "LGN",
        "orbit": None,
    }

    assert telem.fixed_metadata == {
        "platform": "LANDSAT_8",
        "instrument": "OLI_TIRS",
        "product_type": "satellite_telemetry_data",
        "format": "MD",
        "gsi": "LGN",
        "orbit": None,
    }

    # Ingested products carry little of the original metadata...
    assert albers.fixed_metadata == {
        "platform": "LANDSAT_8",
        "instrument": "OLI_TIRS",
        "product_type": "nbar",
        "format": "NetCDF",
        "label": None,
    }


def test_sampled_product_fixed_fields(summary_store: SummaryStore) -> None:
    # Compute fixed fields using a sampled percentage.

    # (We're doing this manually to force it to use table sampling -- our test data is
    # not big enough to trigger it in the `run_generate()` tests)

    # Tiled product, sampled
    fixed_fields = summary_store._find_product_fixed_metadata(
        summary_store.index.products.get_by_name("ga_ls9c_ard_3"),
        sample_datasets_size=5,
    )

    assert fixed_fields == {
        "platform": "landsat-9",
        "instrument": "OLI_TIRS",
        "product_family": "ard",
        "format": "GeoTIFF",
        "eo_gsd": 15.0,
        "label": None,
        "dataset_maturity": "final",
    }


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_generate_empty_time(run_generate, summary_store: SummaryStore) -> None:
    run_generate("ls8_nbar_albers")
    # No datasets in 2018
    assert summary_store.get("ls8_nbar_albers", year=2018) is None, (
        "There should be no datasets in 2018"
    )

    # Year that does not exist for LS8
    summary = summary_store.get("ls8_nbar_albers", year=2006, month=None, day=None)
    assert summary is None


def test_calc_empty(summary_store: SummaryStore) -> None:
    summary_store.refresh_all_product_extents()

    # Should not exist.
    summary = summary_store.get("ls8_fake_product", year=2006, month=None, day=None)
    assert summary is None


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_generate_telemetry(run_generate, summary_store: SummaryStore) -> None:
    """
    Telemetry data polygons can be synthesized from the path/row values
    """
    run_generate("ls8_satellite_telemetry_data")

    _expect_values(
        summary_store.get("ls8_satellite_telemetry_data"),
        dataset_count=1199,
        footprint_count=1199,
        time_range=Range(
            begin=datetime(2016, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2018, 1, 1, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        region_dataset_counts={
            "91": 56,
            "92": 56,
            "93": 56,
            "90": 51,
            "95": 47,
            "94": 45,
            "96": 44,
            "101": 43,
            "98": 43,
            "100": 42,
            "105": 42,
            "111": 42,
            "99": 42,
            "104": 41,
            "110": 41,
            "112": 41,
            "103": 40,
            "107": 40,
            "108": 40,
            "109": 40,
            "89": 40,
            "97": 40,
            "113": 39,
            "102": 37,
            "106": 36,
            "114": 32,
            "116": 29,
            "115": 27,
            "88": 27,
        },
        newest_creation_time=datetime(2017, 12, 31, 3, 38, 43, tzinfo=tzutc()),
        timeline_period="month",
        timeline_count=24,
        crses={"EPSG:4326"},
        size_bytes=10333203380934,
    )


def test_generate_day(run_generate, summary_store: SummaryStore) -> None:
    run_generate("ga_ls8c_ard_3")

    _expect_values(
        summary_store.get("ga_ls8c_ard_3", year=2022, month=7, day=19),
        dataset_count=3,
        footprint_count=3,
        time_range=Range(
            begin=datetime(2022, 7, 19, 0, 0, tzinfo=DEFAULT_TZ),
            end=datetime(2022, 7, 20, 0, 0, tzinfo=DEFAULT_TZ),
        ),
        newest_creation_time=datetime(2022, 8, 23, 14, 56, 45, 940_847, tzinfo=tzutc()),
        timeline_period="day",
        timeline_count=1,
        crses={"EPSG:32656", "EPSG:32652"},
        size_bytes=None,
    )


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_force_dataset_regeneration(
    run_generate, summary_store: SummaryStore, odc_test_db: Datacube
) -> None:
    """
    We should be able to force-replace dataset extents with the "--recreate-dataset-extents" option
    """
    run_generate("ls8_nbar_albers")
    [example_dataset] = list(
        summary_store.index.datasets.search(product="ls8_nbar_albers", limit=1)
    )

    original_footprint = summary_store.get_dataset_footprint_region(example_dataset.id)
    assert original_footprint is not None

    # Now let's break the footprint!
    with summary_store.e_index.engine.begin() as conn:
        conn.execute(
            text(
                f"update {CUBEDASH_SCHEMA}.dataset_spatial "
                "    set footprint="
                "        ST_SetSRID("
                "            ST_GeomFromText("
                "                'POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 42.3903701743239,"
                "                          -71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,"
                "                          -71.1776585052917 42.3902909739571))'"
                "            ),"
                "            4326"
                "        )"
                "    where id=:ds_id"
            ),
            {"ds_id": example_dataset.id},
        )
    # Make sure it worked
    footprint = summary_store.get_dataset_footprint_region(example_dataset.id)
    assert footprint != original_footprint, "Test data didn't successfully override"

    # Now force-recreate dataset extents
    run_generate("-v", "ls8_nbar_albers", "--recreate-dataset-extents")

    # ... and they should be correct again
    footprint = summary_store.get_dataset_footprint_region(example_dataset.id)
    assert footprint == original_footprint, "Dataset extent was not regenerated"


@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_calc_albers_summary_with_storage(summary_store: SummaryStore) -> None:
    # Should not exist yet.
    summary = summary_store.get("ls8_nbar_albers", year=None, month=None, day=None)
    assert summary is None
    summary = summary_store.get("ls8_nbar_albers", year=2017, month=None, day=None)
    assert summary is None

    # We don't want it to add a few minutes overlap buffer,
    # as we add datasets and refresh immediately.
    summary_store.dataset_overlap_carefulness = timedelta(seconds=0)

    # Calculate overall summary
    _, summary = summary_store.refresh("ls8_nbar_albers")

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

    original = summary_store.get("ls8_nbar_albers", 2017)

    # It should now return the same copy, not rebuild it.
    summary_store.refresh("ls8_nbar_albers")

    cached_s = summary_store.get("ls8_nbar_albers", 2017)
    assert original is not cached_s
    assert cached_s.dataset_count == original.dataset_count
    assert cached_s.summary_gen_time is not None
    assert cached_s.summary_gen_time == original.summary_gen_time, (
        "A new, rather than cached, summary was returned"
    )


def test_cubedash_gen_refresh(
    run_generate, odc_test_db: Datacube, empty_client
) -> None:
    """
    cubedash-gen shouldn't increment the product sequence when run normally
    """

    def _get_product_seq_value():
        with odc_test_db.index._active_connection() as conn:
            [new_val] = conn.execute(
                text(f"select last_value from {CUBEDASH_SCHEMA}.product_id_seq;")
            ).fetchone()
            return new_val

    # Once
    run_generate("--all")
    original_value = _get_product_seq_value()

    # Twice
    run_generate("--no-init-database", "--refresh-stats", "--force-refresh", "--all")

    # Value wasn't incremented!
    value_after_rerun = _get_product_seq_value()
    assert value_after_rerun == original_value, (
        "Product sequence was incremented without any new products being added."
    )


def test_computed_regions_match_those_summarised(summary_store: SummaryStore) -> None:
    """
    The region code for all datasets should be computed identically when
    done in both SQL and Python.
    """
    summary_store.refresh_all_product_extents()

    # Loop through all datasets in the test data to check that the the DB and Python
    # functions give identical region codes.
    for product in summary_store.index.products.get_all():
        region_info = GridRegionInfo.for_product(product, None)
        for dataset in summary_store.index.datasets.search(product=product.name):
            (
                footprint,
                alchemy_calculated_region_code,
            ) = summary_store.get_dataset_footprint_region(dataset.id)

            python_calculated_region_code = region_info.dataset_region_code(dataset)
            assert python_calculated_region_code == alchemy_calculated_region_code, (
                "Python and DB calculated region codes differ. "
                f"{python_calculated_region_code!r} != {alchemy_calculated_region_code!r}"
                f"for product {dataset.product.name!r}, dataset {dataset!r}"
            )
