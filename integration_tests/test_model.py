"""
Tests related to the store
"""

import operator
from collections import Counter
from datetime import datetime

from pytest import approx
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from cubedash._model import TimePeriodOverview
from datacube.model import Range

ANTIMERIDIAN_POLY = shape(
    {
        "coordinates": [
            (
                (
                    (4_570_367.383_358_164, -2_686_558.487_741_805_6),
                    (4_596_884.401_015_367, -2_696_233.324_769_045),
                    (4_651_594.703_703_109, -2_546_381.159_980_601_6),
                    (4_843_159.172_919_877, -2_616_889.143_862_793),
                    (4_898_146.635_848_95, -2_468_826.913_216_862),
                    (5_121_388.307_754_11, -2_552_790.834_339_854),
                    (5_038_978.335_865_037, -2_770_349.723_049_303_5),
                    (5_004_954.987_359_211, -2_757_509.068_020_739),
                    (4_948_970.709_999_509, -2_906_354.495_410_728_3),
                    (4_915_016.893_557_812, -2_893_690.200_226_952),
                    (4_859_111.937_490_443, -3_043_145.488_673_557_5),
                    (4_484_058.430_363_337, -2_903_082.593_313_134),
                    (4_563_878.626_931_71, -2_684_193.641_291_883_3),
                    (4_570_367.383_358_164, -2_686_558.487_741_805_6),
                ),
            )
        ],
        "type": "MultiPolygon",
    }
)

EXPECTED_CLEAN_POLY = shape(
    {
        "coordinates": (
            (
                (175.922_881_766_291_77, -17.736_861_402_938_633),
                (176.188_837_471_027_1, -17.738_137_837_459_98),
                (176.195_037_831_375_5, -16.292_861_346_068_93),
                (178.096_026_193_380_06, -16.291_568_154_707_434),
                (178.088_436_034_659_25, -14.854_334_383_996_104),
                # >180 is permitted in GeoJSON.
                # Eg. https://github.com/mapbox/mapbox-gl-js/issues/3250#issuecomment-249389420
                (180.284_040_231_688_78, -14.833_444_366_027_617),
                (180.318_554_911_528_2, -16.948_097_832_991_902),
                (179.978_135_239_479_34, -16.953_369_544_929_96),
                (180.001_921_070_416_6, -18.391_284_153_387_748),
                (179.658_683_869_298_05, -18.396_970_158_923_263),
                (179.681_782_863_397_9, -19.834_509_550_513_136),
                (175.843_557_972_428_02, -19.837_373_571_581_587),
                (175.857_811_163_213_48, -17.736_490_298_350_088),
                (175.922_881_766_291_77, -17.736_861_402_938_633),
            ),
        ),
        "type": "Polygon",
    }
)


def test_footprint_antimeridian():
    """
    When a polygon crosses the antimeridian, check that it's translated correctly.

    Existing integration tests check "normal" polygons already, so we're not repeating that here.
    """
    overview = TimePeriodOverview(
        dataset_count=1,
        timeline_dataset_counts=Counter("abc"),
        region_dataset_counts=Counter("abc"),
        timeline_period="dummy value",
        time_range=Range("2000-01-01", "2001-01-01"),
        footprint_geometry=ANTIMERIDIAN_POLY,
        footprint_crs="EPSG:3577",
        footprint_count=1,
        newest_dataset_creation_time=datetime.now(),
        crses=set(),
        summary_gen_time=datetime.now(),
        size_bytes=256,
    )

    footprint_latlon = overview.footprint_wrs84
    assert footprint_latlon.is_valid, "Expected valid footprint"

    assert_shapes_mostly_equal(footprint_latlon, EXPECTED_CLEAN_POLY, 0.001)


def assert_shapes_mostly_equal(
    shape1: BaseGeometry, shape2: BaseGeometry, threshold: float
):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    # Check area first, as it's a nicer error message when they're wildly different.
    assert shape1.area == approx(
        shape2.area, abs=threshold
    ), "Shapes have different areas"

    s1 = shape1.simplify(tolerance=threshold)
    s2 = shape2.simplify(tolerance=threshold)
    assert s1 == s2, f"{s1} is not mostly equal to {s2}"
