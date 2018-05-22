import time
from datetime import datetime

from dateutil.tz import tzutc

from cubedash import _model
from datacube.model import Range


def main():
    t = time.time()

    s = _model.DEFAULT_STORE.calculate_summary(
        "ls8_nbar_albers", Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
    )
    print(repr(s))
    assert s.dataset_count == 9244
    assert s.footprint_count == 9244
    assert s.newest_dataset_creation_time == datetime(
        2017, 12, 7, 1, 24, 58, 134_221, tzinfo=tzutc()
    )

    s = _model.DEFAULT_STORE.calculate_summary(
        "ls8_nbar_scene", Range(datetime(2017, 4, 1), datetime(2017, 5, 1))
    )
    print(repr(s))
    assert s.dataset_count == 1024
    assert s.footprint_count == 1024
    assert s.newest_dataset_creation_time == datetime(
        2017, 7, 4, 11, 19, 33, tzinfo=tzutc()
    )

    # Year with invalid shapely polygons
    s = _model.DEFAULT_STORE.update("ls7_nbar_albers", 2000, None, None)
    print(repr(s))
    assert s.dataset_count == 62988
    assert s.footprint_count == 62988
    assert s.newest_dataset_creation_time == datetime(
        2017, 2, 26, 10, 22, 45, 256_080, tzinfo=tzutc()
    )
    assert s.period == "day"
    assert s.time_range == Range(
        begin=datetime(2000, 1, 1, 0, 0), end=datetime(2001, 1, 1, 0, 0)
    )

    # A whole time range. Many invalid extents.
    s = _model.DEFAULT_STORE.update("low_tide_comp_20p", None, None, None)
    print(repr(s))
    assert s.dataset_count == 61812
    assert s.footprint_count == 61812
    assert s.newest_dataset_creation_time == datetime(
        2017, 6, 14, 2, 25, 33, 600_040, tzinfo=tzutc()
    )
    assert s.period == "day"
    assert s.time_range == Range(
        begin=datetime(2000, 1, 1, 0, 0), end=datetime(2016, 11, 1, 0, 0)
    )

    print(f"Finished in {time.time()-t}")


if __name__ == "__main__":
    main()
