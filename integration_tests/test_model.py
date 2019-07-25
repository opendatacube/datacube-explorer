"""
Tests related to the store
"""

import json
from collections import Counter
from shapely.geometry import shape
from cubedash._model import TimePeriodOverview
from datacube.model import Range
import pyproj
from datetime import datetime


def test_footprint_antimeridian():
    with open("integration_tests/data/antimeridean-polygon.json") as f:
        geometry = json.load(f)["geometry"]
    footprint = shape(geometry)
    print(footprint)

    overview = TimePeriodOverview(
        dataset_count=1,
        timeline_dataset_counts=Counter('abc'),
        region_dataset_counts=Counter('abc'),
        timeline_period='dummy value',
        time_range=Range('2000-01-01', '2001-01-01'),
        footprint_geometry=footprint,
        footprint_crs='EPSG:3577',
        footprint_count=1,
        newest_dataset_creation_time=datetime.now(),
        crses=set(),
        summary_gen_time=datetime.now(),
        size_bytes=256
    )

    footprint_latlon = overview.footprint_wrs84
    origin = pyproj.Proj(init=overview.footprint_crs)
    dest = pyproj.Proj(init="epsg:4326")

    assert overview._test_wrap_coordinates(overview, footprint_latlon, origin, dest) is False
