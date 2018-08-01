from collections import Counter
from datetime import datetime

from dateutil import tz
from shapely import geometry as geo
from sqlalchemy import bindparam, select
from sqlalchemy.dialects import postgresql as postgres

from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._schema import PgGridCell
from cubedash.summary._stores import ProductSummary
from cubedash.summary._summarise import GridCell
from datacube.model import Range


def _overview():
    orig = TimePeriodOverview(
        dataset_count=1234,
        timeline_dataset_counts=Counter(
            [
                datetime(2017, 1, 2, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 3, tzinfo=tz.tzutc()),
                datetime(2017, 1, 1, tzinfo=tz.tzutc()),
            ]
        ),
        grid_dataset_counts=Counter([GridCell(1, 2), GridCell(1, 2), GridCell(3, 4)]),
        timeline_period="day",
        time_range=Range(
            datetime(2017, 1, 2, tzinfo=tz.tzutc()),
            datetime(2017, 2, 3, tzinfo=tz.tzutc()),
        ),
        footprint_geometry=geo.Polygon(
            [
                # ll:
                (-29.882_024, 113.105_949),
                # lr:
                (-29.930_607, 115.464_187),
                # ur:
                (-27.849_244, 115.494_523),
                # ul
                (-27.804_641, 113.18267),
            ]
        ),
        footprint_count=0,
        newest_dataset_creation_time=datetime(2018, 1, 1, 1, 1, 1, tzinfo=tz.tzutc()),
        crses={"epsg:1234"},
        size_bytes=123_400_000,
    )
    return orig


def test_get_null(summary_store: SummaryStore):
    """
    An area with nothing generated should come back as null.

    (It's important for us to distinguish between an area with zero datasets
    and an area where the summary/extent has not been generated.)
    """
    loaded = summary_store.get("some_product", 2019, 4, None)
    assert loaded is None


def test_srid_lookup(summary_store: SummaryStore):
    srid = summary_store._target_srid()
    assert srid is not None
    assert isinstance(srid, int)

    srid2 = summary_store._target_srid()
    assert srid == srid2

    assert summary_store._get_srid_name(srid) == "EPSG:4326"

    # Cached?
    cache_hits = summary_store._get_srid_name.cache_info().hits
    assert summary_store._get_srid_name(srid) == "EPSG:4326"
    assert summary_store._get_srid_name.cache_info().hits > cache_hits


def test_put_get_summaries(summary_store: SummaryStore):
    """
    Test the serialisation/deserialisation from postgres
    """
    o = _overview()
    product_name = "some_product"
    summary_store._set_product_extent(
        ProductSummary(product_name, 4321, datetime(2017, 1, 1), datetime(2017, 4, 1))
    )
    summary_store._put(product_name, 2017, None, None, o)
    loaded = summary_store.get(product_name, 2017, None, None)

    assert o is not loaded, (
        "Store should not return the original objects " "(they may change)"
    )

    o.dataset_count = 4321
    o.newest_dataset_creation_time = datetime(2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc())
    summary_store._put(product_name, 2017, None, None, o)

    loaded = summary_store.get(product_name, 2017, None, None)
    assert loaded.dataset_count == 4321
    assert loaded.newest_dataset_creation_time == datetime(
        2018, 2, 2, 2, 2, 2, tzinfo=tz.tzutc()
    )


def test_gridcell_type(summary_store: SummaryStore):
    # This will both serialise and deserialise
    cell = bindparam("ourcell", type_=PgGridCell)
    row = summary_store._engine.execute(
        select([cell]), ourcell=GridCell(3, 4)
    ).fetchone()
    [cell] = row
    assert cell == GridCell(3, 4)

    # Inside an array
    cell = bindparam("ourcells", type_=postgres.ARRAY(PgGridCell))
    row = summary_store._engine.execute(
        select([cell]), ourcells=[GridCell(1, 2), GridCell(3, 4)]
    ).fetchone()
    [cell] = row
    assert cell == [GridCell(1, 2), GridCell(3, 4)]
