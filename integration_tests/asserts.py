import json
import re
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import Dict, Tuple, Set, Optional

from dateutil.tz import tzutc
from flask import Response
from flask.testing import FlaskClient
from requests_html import HTML

from cubedash._utils import default_utc
from cubedash.summary import TimePeriodOverview
from datacube.model import Range
from datacube.utils import validate_document

# GeoJSON schema from http://geojson.org/schema/FeatureCollection.json
_FEATURE_COLLECTION_SCHEMA_PATH = Path(__file__).parent / 'schemas' / 'FeatureCollection.json'
_FEATURE_COLLECTION_SCHEMA = json.load(_FEATURE_COLLECTION_SCHEMA_PATH.open('r'))


def get_geojson(client: FlaskClient, url: str) -> Dict:
    data = get_json(client, url)
    validate_document(
        data,
        _FEATURE_COLLECTION_SCHEMA,
        schema_folder=_FEATURE_COLLECTION_SCHEMA_PATH.parent,
    )
    return data


def get_html_response(client: FlaskClient, url: str) -> Tuple[HTML, Response]:
    response: Response = client.get(url)
    assert response.status_code == 200, response.data.decode('utf-8')
    html = HTML(html=response.data.decode('utf-8'))
    return html, response


def get_json(client: FlaskClient, url: str) -> Dict:
    rv: Response = client.get(url)
    try:
        assert rv.status_code == 200, rv.data
        assert rv.is_json, "Expected json content type in response"
        data = rv.json
        assert data is not None, "Empty response from server"
    except AssertionError:
        pprint(rv.data)
        raise
    return data


def get_html(client: FlaskClient, url: str) -> HTML:
    html, _ = get_html_response(client, url)
    return html


def check_area(area_pattern, html):
    assert re.match(area_pattern + r' \(approx', html.find('.coverage-footprint-area', first=True).text)


def check_last_processed(html, time):
    __tracebackhide__ = True
    assert html.find('.last-processed time', first=True).attrs['datetime'].startswith(time)


def check_dataset_count(html, count: int):
    __tracebackhide__ = True
    actual = html.find('.dataset-count', first=True).text
    expected = '{:,d}'.format(count)
    assert f'{expected} dataset' in actual, \
        f"Incorrect dataset count: found {actual} instead of {expected}"


def expect_values(s: TimePeriodOverview,
                  dataset_count: int,
                  footprint_count: int,
                  time_range: Range,
                  newest_creation_time: datetime,
                  timeline_period: str,
                  timeline_count: int,
                  crses: Set[str],
                  size_bytes: Optional[int]):
    __tracebackhide__ = True

    was_timeline_error = False
    try:
        assert s.dataset_count == dataset_count, "wrong dataset count"
        assert s.footprint_count == footprint_count, "wrong footprint count"
        if s.footprint_count is not None and s.footprint_count > 0:
            assert s.footprint_geometry is not None, "No footprint, despite footprint count"
            assert s.footprint_geometry.area > 0, "Empty footprint"

        assert s.time_range == time_range, "wrong dataset time range"
        assert s.newest_dataset_creation_time == default_utc(
            newest_creation_time
        ), "wrong newest dataset creation"
        assert s.timeline_period == timeline_period, (
            f"Should be a {timeline_period}, "
            f"not {s.timeline_period} timeline"
        )

        assert s.summary_gen_time is not None, (
            "Missing summary_gen_time (there's a default)"
        )

        assert s.crses == crses, "Wrong dataset CRSes"

        if size_bytes is None:
            assert s.size_bytes is None, "Expected null size_bytes"
        else:
            assert s.size_bytes == size_bytes, "Wrong size_bytes"

        assert s.summary_gen_time is not None, "Missing summary_gen_time"

        was_timeline_error = True
        if s.timeline_dataset_counts is None:
            if timeline_count is not None:
                raise AssertionError(
                    f"null timeline_dataset_counts. "
                    f"Expected entry with {timeline_count} records."
                )
        else:
            assert len(s.timeline_dataset_counts) == timeline_count, (
                "wrong timeline entry count"
            )

            assert sum(s.region_dataset_counts.values()) == s.dataset_count, (
                "region dataset count doesn't match total dataset count"
            )
            assert sum(s.timeline_dataset_counts.values()) == s.dataset_count, (
                "timeline count doesn't match dataset count"
            )
        was_timeline_error = False

    except AssertionError as a:
        print(f"""Got:
        dataset_count {s.dataset_count}
        footprint_count {s.footprint_count}
        time range:
            - {repr(s.time_range.begin.astimezone(tzutc()))}
            - {repr(s.time_range.end.astimezone(tzutc()))}
        newest: {repr(s.newest_dataset_creation_time.astimezone(tzutc()))}
        crses: {repr(s.crses)}
        size_bytes: {s.size_bytes}
        timeline
            period: {s.timeline_period}
            dataset_counts: {None if s.timeline_dataset_counts is None else len(s.timeline_dataset_counts)}
        """)
        if was_timeline_error:
            print("timeline keys:")
            for day, count in s.timeline_dataset_counts.items():
                print(f"\t{repr(day)}: {count}")
        raise
