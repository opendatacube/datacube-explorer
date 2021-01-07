"""
Unit test for re-cursive geometry filtering
"""
import json

from shapely.geometry import mapping, shape

from cubedash.summary._model import _filter_geom

def test_filter_geom():
    assert _filter_geom([]) == []
    geom = shape(json.loads('{"type": "Point", "coordinates": [0.0, 0.0]}'))
    assert _filter_geom([geom])
