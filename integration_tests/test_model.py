"""
Tests related to the store
"""
import operator
from collections import Counter
from datetime import datetime, date

import pytest
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

# Above poly reprojected to wgs84 & cut at the antimeridian.
EXPECTED_CLEAN_POLY = shape(
    {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [175.92288176629177, -17.736861402938633],
                    [176.1888374710271, -17.73813783746],
                    [176.19274112300351, -16.832870187587258],
                    [176.1950378313755, -16.29286134606892],
                    [177.12631421327535, -16.295673531224818],
                    [178.05758316421358, -16.291867516947526],
                    [178.09602619338006, -16.291568154707434],
                    [178.0912004166552, -15.382375916093759],
                    [178.08843603465925, -14.854334383996104],
                    [179.00907208358686, -14.850127664424102],
                    [179.92959914255476, -14.839348217675006],
                    [180.0, -14.8382531679417],
                    [180.0, -14.901145935058606],
                    [180.0, -15.001144409179695],
                    [180.0, -15.101142883300781],
                    [180.0, -15.201141357421871],
                    [180.0, -15.301139831542983],
                    [180.0, -15.401138305664036],
                    [180.0, -15.501136779785147],
                    [180.0, -15.601135253906271],
                    [180.0, -15.701133728027378],
                    [180.0, -15.801132202148432],
                    [180.0, -15.901130676269526],
                    [180.0, -16.001129150390614],
                    [180.0, -16.1011276245117],
                    [180.0, -16.2011260986328],
                    [180.0, -16.301124572753903],
                    [180.0, -16.401123046875046],
                    [180.0, -16.501121520996055],
                    [180.0, -16.60111999511723],
                    [180.0, -16.70111846923827],
                    [180.0, -16.801116943359357],
                    [180.0, -16.901115417480494],
                    [180.0, -16.95305734755206],
                    [179.97813523947934, -16.953369544929924],
                    [179.99302728742177, -17.858226072677454],
                    [180.0, -18.276599030283858],
                    [180.0, -18.301094055175813],
                    [180.0, -18.391318424361483],
                    [179.65868386929805, -18.396970158923292],
                    [179.67309532879375, -19.298430001279396],
                    [179.6817828633979, -19.834509550513136],
                    [178.72314470554812, -19.84536174472205],
                    [177.7643869324005, -19.849464207437887],
                    [176.8056175226244, -19.846816427833463],
                    [175.84694446275205, -19.83741873562826],
                    [175.84355797242802, -19.837373571581587],
                    [175.8497361181137, -18.936982907437624],
                    [175.85582291449427, -18.034627062034698],
                    [175.85781116321348, -17.736490298350127],
                    [175.92288176629177, -17.736861402938633],
                ]
            ],
            [
                [
                    [-180.0, -14.8382531679417],
                    [-179.71595976831122, -14.833444366027607],
                    [-179.7012667401856, -15.744141801731066],
                    [-179.6863589113362, -16.652206480636824],
                    [-179.6814450884718, -16.948097832991902],
                    [-180.0, -16.95305734755206],
                    [-180.0, -16.901115417480494],
                    [-180.0, -16.801116943359357],
                    [-180.0, -16.70111846923827],
                    [-180.0, -16.60111999511723],
                    [-180.0, -16.501121520996055],
                    [-180.0, -16.401123046875046],
                    [-180.0, -16.301124572753903],
                    [-180.0, -16.2011260986328],
                    [-180.0, -16.1011276245117],
                    [-180.0, -16.001129150390614],
                    [-180.0, -15.901130676269526],
                    [-180.0, -15.801132202148432],
                    [-180.0, -15.701133728027378],
                    [-180.0, -15.601135253906271],
                    [-180.0, -15.501136779785147],
                    [-180.0, -15.401138305664036],
                    [-180.0, -15.301139831542983],
                    [-180.0, -15.201141357421871],
                    [-180.0, -15.101142883300781],
                    [-180.0, -15.001144409179695],
                    [-180.0, -14.901145935058606],
                    [-180.0, -14.8382531679417],
                ]
            ],
            [
                [
                    [-180.0, -18.276599030283858],
                    [-179.9980789295834, -18.391284153387748],
                    [-180.0, -18.391318424361483],
                    [-180.0, -18.301094055175813],
                    [-180.0, -18.276599030283858],
                ]
            ],
        ],
    }
)


def _create_overview():
    overview = TimePeriodOverview(
        product_name="test_model_product",
        year=None,
        month=None,
        day=None,
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
        product_refresh_time=datetime.now(),
    )
    return overview


def test_footprint_antimeridian(benchmark):
    """
    When a polygon crosses the antimeridian, check that it's translated correctly.
    """
    overview = _create_overview()

    footprint_latlon = benchmark(lambda: overview.footprint_wgs84)
    assert_shapes_mostly_equal(footprint_latlon, EXPECTED_CLEAN_POLY, 0.1)


def test_footprint_normal(benchmark):
    # A normal poly that doesn't cross antimeridian.
    normal_poly = shape(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-1_100_000.0, -1_800_000.0],
                    [-1_100_000.0, -1_820_678.597_382_843_7],
                    [-1_184_944.706_234_691_7, -1_814_708.549_002_366_4],
                    [-1_188_914.681_756_198_4, -1_814_413.718_946_547_2],
                    [-1_189_815.686_895_983_5, -1_802_644.727_911_235_5],
                    [-1_189_748.998_910_200_8, -1_801_865.255_277_810_9],
                    [-1_189_494.857_669_78, -1_800_000.0],
                    [-1_100_000.0, -1_800_000.0],
                ]
            ],
        }
    )
    expected_poly = shape(
        {
            "coordinates": (
                (
                    (121.728_481_294_995, -16.493_157_200_887_75),
                    (121.712_966_884_510_11, -16.680_127_187_578_158),
                    (120.927_236_349_784_75, -16.561_834_079_973_22),
                    (120.890_564_152_335_9, -16.556_046_811_339_68),
                    (120.891_725_246_251_8, -16.448_956_170_181_916),
                    (120.892_975_809_931_98, -16.441_961_802_367_327),
                    (120.896_846_007_609_55, -16.425_298_327_691_696),
                    (121.728_481_294_995, -16.493_157_200_887_75),
                ),
            ),
            "type": "Polygon",
        }
    )

    o = _create_overview()
    o.footprint_geometry = normal_poly
    res: BaseGeometry = benchmark(lambda: o.footprint_wgs84)
    assert_shapes_mostly_equal(res, expected_poly, 0.001)


def assert_shapes_mostly_equal(
    shape1: BaseGeometry, shape2: BaseGeometry, threshold: float
):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    # Check area first, as it's a nicer error message when they're wildly different.
    assert shape1.area == pytest.approx(
        shape2.area, abs=threshold
    ), "Shapes have different areas"

    s1 = shape1.simplify(tolerance=threshold)
    s2 = shape2.simplify(tolerance=threshold)
    assert (s1 - s2).area < threshold, f"{s1} is not mostly equal to {s2}"


def test_computed_properties():
    o = _create_overview()
    o.product_name = "test_product"

    def check_flat_period(o, expected_period: str, expected_date: date):
        assert o.as_flat_period() == (expected_period, expected_date)

        # Converting the other way should also match.
        unflattened = TimePeriodOverview.from_flat_period_representation(
            *o.as_flat_period()
        )
        assert (o.year, o.month, o.day) == unflattened

    assert o.label == "test_product all all all"
    check_flat_period(o, "all", date(1900, 1, 1))
    assert str(o) == "test_product all all all (1 dataset)"

    o.year = 2018
    assert o.label == "test_product 2018 all all"
    check_flat_period(o, "year", date(2018, 1, 1))

    o.month = 4
    assert o.label == "test_product 2018 4 all"
    check_flat_period(o, "month", date(2018, 4, 1))

    o.day = 6
    assert o.label == "test_product 2018 4 6"
    check_flat_period(o, "day", date(2018, 4, 6))

    o.dataset_count = 321
    assert str(o) == "test_product 2018 4 6 (321 datasets)"
