"""
Unit test for re-cursive geometry filtering
"""
import json
from pathlib import Path

import pytest
import shapely.wkt
from shapely.geometry import shape

from cubedash.summary._model import _filter_geom, _polygon_chain

TEST_DATA_DIR = Path(__file__).parent / "data"


class ValidGeometries:
    def __init__(self, footprint_geometry):
        self.footprint_geometry = footprint_geometry


@pytest.fixture()
def testing_polygon():
    sample_geometry_file = open(TEST_DATA_DIR / "unary_union_fail_sample.txt")
    line_c = 0
    shapely_polygon = []
    for line in sample_geometry_file:
        if len(line) > 1:
            line_c += 1
            if 16 < line_c < 50:  # min amount for testing
                poly = shapely.wkt.loads(line)
                shapely_polygon.append(ValidGeometries(poly))
    assert len(shapely_polygon) == 33
    return shapely_polygon


def test_filter_geom():
    assert _filter_geom([]) == []
    geom = shape(json.loads('{"type": "Point", "coordinates": [0.0, 0.0]}'))
    assert _filter_geom([geom])


@pytest.mark.skip("Skipping because the newer Shapely is handling geometry better.")
def test_nested_exception(testing_polygon):
    """
    simulating the behaviour in _model.py
    """
    with pytest.raises(
        ValueError,
        match="No Shapely geometry can be created from null value",
    ):
        geometry_union = shapely.ops.unary_union(
            [ele.footprint_geometry for ele in testing_polygon]
        )
    with pytest.raises(
        ValueError,
        match="No Shapely geometry can be created from null value",
    ):
        geometry_union = shapely.ops.unary_union(
            [ele.footprint_geometry.buffer(0.00) for ele in testing_polygon]
        )

    polygonlist = _polygon_chain(testing_polygon)
    assert type(polygonlist) is list
    assert len(polygonlist) == 262
    filtered_geom = _filter_geom(polygonlist)
    assert len(filtered_geom) == 199
    geometry_union = shapely.ops.unary_union(filtered_geom)

    assert geometry_union.is_valid
