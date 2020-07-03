from datetime import datetime
from pathlib import Path
from pprint import pprint
from uuid import UUID

from dateutil import tz

import pytest
from cubedash.summary import _extents
from datacube.index import Index
from geoalchemy2.shape import to_shape

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def eo3_index(module_dea_index: Index, dataset_loader):

    loaded = dataset_loader(
        "usgs_ls5t_level1_1",
        TEST_DATA_DIR / "LT05_L1TP_113081_19880330_20170209_01_T1.odc-metadata.yaml",
    )
    assert loaded == 1

    loaded = dataset_loader(
        "ga_ls5t_ard_3",
        TEST_DATA_DIR
        / "ga_ls5t_ard_3-1-20200605_113081_1988-03-30_final.odc-metadata.yaml",
    )
    assert loaded == 1

    return module_dea_index


def test_eo3_extents(eo3_index: Index):
    """
    Do we extract the elements of an EO3 extent properly?

    (ie. not the older grid_spatial definitions)
    """
    [dataset_extent_row] = _extents.get_sample_dataset("ga_ls5t_ard_3", index=eo3_index)
    pprint(dataset_extent_row)

    assert dataset_extent_row["id"] == UUID("5b2f2c50-e618-4bef-ba1f-3d436d9aed14")

    assert dataset_extent_row["center_time"] == datetime(
        1988, 3, 30, 1, 41, 16, 855723, tzinfo=tz.tzutc()
    )
    assert dataset_extent_row["creation_time"] == datetime(
        2020, 6, 5, 7, 15, 26, 599544, tzinfo=tz.tzutc()
    )
    assert (
        dataset_extent_row["dataset_type_ref"]
        == eo3_index.products.get_by_name("ga_ls5t_ard_3").id
    )

    # This should be the geometry field of eo3, not the max/min bounds
    # that eo1 compatibility adds within `grid_spatial`.
    footprint = to_shape(dataset_extent_row["footprint"])
    assert footprint.__geo_interface__ == {
        "type": "Polygon",
        "coordinates": (
            (
                (271725.0, -3248955.0),
                (271545.69825676165, -3249398.5651073675),
                (269865.69825676165, -3257048.5651073675),
                (260385.671713869, -3301028.687185048),
                (243345.665287001, -3380468.71711744),
                (234975.6600318394, -3419708.7417040393),
                (233985.0, -3424865.9692269894),
                (233985.0, -3427879.8526289104),
                (238960.4382844724, -3428684.6511509297),
                (426880.46102441975, -3457454.6546404576),
                (427870.50583861716, -3457604.6614651266),
                (428083.1938027047, -3457585.2522918927),
                (428204.3403041278, -3457251.2567206817),
                (465584.3403041278, -3281961.2567206817),
                (466034.48616560805, -3279560.5286560515),
                (466004.93355473573, -3279073.0044296845),
                (465859.7539769853, -3279015.379066476),
                (461689.60011989495, -3278355.3547828994),
                (271725.0, -3248955.0),
            ),
        ),
    }
    assert footprint.is_valid, "Created footprint is not a valid geometry"
    assert (
        dataset_extent_row["footprint"].srid == 32650
    ), "Expected epsg:32650 within the footprint geometry"

    assert dataset_extent_row["region_code"] == "113081"
    assert dataset_extent_row["size_bytes"] is None
