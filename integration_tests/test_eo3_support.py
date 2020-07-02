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
    [dataset_extent_row] = _extents.get_sample_dataset(
        "usgs_ls5t_level1_1", index=eo3_index
    )
    pprint(dataset_extent_row)

    assert dataset_extent_row["id"] == UUID("9989545f-906d-5090-a38e-cdbfbfc1afca")

    assert dataset_extent_row["center_time"] == datetime(
        1988, 3, 30, 1, 41, 16, 892044, tzinfo=tz.tzutc()
    )
    assert dataset_extent_row["creation_time"] == datetime(
        2017, 2, 9, 8, 14, 26, tzinfo=tz.tzutc()
    )
    assert (
        dataset_extent_row["dataset_type_ref"]
        == eo3_index.products.get_by_name("usgs_ls5t_level1_1").id
    )

    # This should be the geometry field of eo3, not the max/min bounds
    # that eo1 compatibility adds within `grid_spatial`.
    footprint = to_shape(dataset_extent_row["footprint"])
    assert footprint.__geo_interface__ == {
        "type": "Polygon",
        "coordinates": (
            (
                (233985.0, -3248685.0),
                (233985.0, -3458115.0),
                (467715.0, -3458115.0),
                (467715.0, -3248685.0),
                (233985.0, -3248685.0),
            ),
        ),
    }
    assert footprint.is_valid, "Created footprint is not a valid geometry"
    assert (
        dataset_extent_row["footprint"].srid == 32650
    ), "Expected epsg:32650 within the footprint geometry"

    # TODO: eo3 region codes
    # assert dataset_extent_row["region_code"] == "113081"
    assert dataset_extent_row["region_code"] is None

    assert dataset_extent_row["size_bytes"] is None
