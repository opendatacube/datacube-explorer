"""
Unit test for re-cursive geometry filtering
"""
import json
from pathlib import Path

from shapely.geometry import shape
import shapely.wkt
import pytest

from cubedash.summary._model import _filter_geom, _polygon_chain

TEST_DATA_DIR = Path(__file__).parent / "data"


class Valid_geometries:
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
            if line_c > 16 and line_c < 50:  # min amount for testing
                poly = shapely.wkt.loads(line)
                shapely_polygon.append(Valid_geometries(poly))
    assert len(shapely_polygon) == 33
    return shapely_polygon


def test_filter_geom():
    assert _filter_geom([]) == []
    geom = shape(json.loads('{"type": "Point", "coordinates": [0.0, 0.0]}'))
    assert _filter_geom([geom])


def test_nested_exception(testing_polygon):
    """
    simulating the behaviour in _model.py
    """
    geometry_union = None
    try:
        geometry_union = shapely.ops.unary_union(
            [ele.footprint_geometry for ele in testing_polygon]
        )
    except ValueError:
        assert geometry_union is None
        try:
            geometry_union = shapely.ops.unary_union(
                [ele.footprint_geometry.buffer(0.00) for ele in testing_polygon]
            )

        except ValueError:
            assert geometry_union is None
            polygonlist = _polygon_chain(testing_polygon)
            assert type(polygonlist) is list
            assert len(polygonlist) == 262
            filtered_geom = _filter_geom(polygonlist)
            assert len(filtered_geom) == 199
            geometry_union = shapely.ops.unary_union(filtered_geom)

            assert geometry_union.is_valid
