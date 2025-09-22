import json
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from pprint import pformat, pprint
from textwrap import indent
from types import TracebackType

import jsonschema
import pytest
from datacube.model import Range
from datacube.utils import InvalidDocException, validate_document
from deepdiff import DeepDiff
from flask.testing import FlaskClient
from selectolax.lexbor import LexborHTMLParser, LexborNode
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from werkzeug.test import TestResponse

from cubedash._utils import default_utc
from cubedash.summary import TimePeriodOverview

# GeoJSON schema from https://geojson.org/schema/FeatureCollection.json


_FEATURE_COLLECTION_SCHEMA_PATH = (
    Path(__file__).parent / "schemas/geojson.org/schema/FeatureCollection.json"
)
_FEATURE_COLLECTION_SCHEMA = json.load(_FEATURE_COLLECTION_SCHEMA_PATH.open("r"))


def assert_shapes_mostly_equal(
    shape1: BaseGeometry, shape2: BaseGeometry, threshold: float
) -> None:
    # __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    # Check area first, as it's a nicer error message when they're wildly different.
    assert shape1.area == pytest.approx(shape2.area, abs=threshold), (
        "Shapes have different areas"
    )

    s1 = shape1.simplify(tolerance=threshold)
    s2 = shape2.simplify(tolerance=threshold)
    assert (s1 - s2).area < threshold, f"{s1} is not mostly equal to {s2}"


def assert_matching_eo3(actual_doc: dict, expected_doc: dict) -> None:
    """
    Assert an EO3 document matches an expected document,

    (without caring about float precision etc.)
    """
    # __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    actual_doc = dict(actual_doc)
    expected_doc = dict(expected_doc)

    # Compare geometry separately (as a parsed shape)
    actual_geom = shape(actual_doc.pop("geometry"))
    expected_geom = shape(expected_doc.pop("geometry"))
    assert_shapes_mostly_equal(actual_geom, expected_geom, 0.0001)

    # Replace expected bbox points with approximates.
    # (We don't worry about float rounding issues)
    actual_doc["bbox"] = [pytest.approx(p) for p in actual_doc["bbox"]]
    expected_doc["bbox"] = [pytest.approx(p) for p in expected_doc["bbox"]]

    # Do the remaining fields match?
    # (note that we have installed a nicer dict comparison in our pytest config)
    assert actual_doc == expected_doc, "\n".join(
        format_doc_diffs(actual_doc, expected_doc)
    )


def get_geojson(client: FlaskClient, url: str) -> dict:
    data = get_json(client, url)
    validate_document(
        data,
        _FEATURE_COLLECTION_SCHEMA,
        schema_folder=_FEATURE_COLLECTION_SCHEMA_PATH.parent,
    )
    return data


def get_text_response(
    client: FlaskClient, url: str, expect_status_code=200
) -> tuple[str, TestResponse]:
    response = client.get(url, follow_redirects=True)
    assert response.status_code == expect_status_code, (
        f"Expected status {expect_status_code} not {response.status_code}."
        f"\nGot:\n{indent(response.data.decode('utf-8'), ' ' * 6)}"
    )
    return response.data.decode("utf-8"), response


def get_json(client: FlaskClient, url: str, expect_status_code=200) -> dict:
    rv = client.get(url, follow_redirects=True)
    try:
        assert rv.status_code == expect_status_code, (
            f"Expected status {expect_status_code} not {rv.status_code}."
            f"\nGot:\n{indent(rv.data.decode('utf-8'), ' ' * 6)}"
        )
        assert rv.is_json, "Expected JSON content type in response"
        data = rv.json
        assert data is not None, "Empty response from server"
    except AssertionError:
        pprint(rv.data)
        raise
    return data


def get_normalized_text(node: LexborNode | LexborHTMLParser) -> str:
    """Extract text from a selectolax node, normalizes whitespace."""
    if node is None:
        return ""
    # Strip whitespace, join lines with single space, strip overall.
    return " ".join(node.strip() for node in node.text(strip=True).splitlines()).strip()


def assert_text_equals(
    node: LexborNode | LexborHTMLParser, expected_text: str, selector: str | None = None
):
    """Assert that the text content of the first matching element is exactly equal to expected_text."""
    __tracebackhide__ = True
    if selector is not None:
        node = node.css_first(selector, strict=True)
    actual_text = get_normalized_text(node)
    assert actual_text == expected_text, (
        f"Text mismatch for selector '{selector}':\n"
        f"  Expected: '{expected_text}'\n"
        f"  Actual:   '{actual_text}'"
    )


def assert_text_contains(
    node: LexborNode | LexborHTMLParser,
    expected_substring: str,
    selector: str | None = None,
):
    """Assert that the text content of the first matching element contains the expected_substring."""
    __tracebackhide__ = True
    if selector is not None:
        node = node.css_first(selector, strict=True)
    actual_text = get_normalized_text(node)
    assert expected_substring in actual_text, (
        f"Substring not found for selector '{selector}':\n"
        f"  Expected to contain: '{expected_substring}'\n"
        f"  Actual text:         '{actual_text}'"
    )


def get_html(client: FlaskClient, url: str) -> LexborHTMLParser:
    response = client.get(url, follow_redirects=True)
    assert response.status_code == 200, response.data.decode("utf-8")
    return LexborHTMLParser(response.data.decode("utf-8"))


def check_area(area_pattern, html: LexborNode | LexborHTMLParser) -> None:
    assert re.match(
        area_pattern + r" \(approx", html.css_first(".coverage-footprint-area").text()
    )


def check_last_processed(html: LexborNode | LexborHTMLParser, time: str) -> None:
    __tracebackhide__ = True
    when = html.css_first(".last-processed time").attributes["datetime"]
    assert when is not None
    assert when.startswith(time)


def check_dataset_count(html: LexborNode | LexborHTMLParser, count: int) -> None:
    __tracebackhide__ = True
    actual = html.css_first(".dataset-count")
    actual = get_normalized_text(actual)
    expected = f"{count:,d}"
    assert f"{expected} dataset" in actual, (
        f"Incorrect dataset count: found {actual} instead of {expected}"
    )


def check_datesets_page_datestring(
    html: LexborNode | LexborHTMLParser, datestring: str
) -> None:
    __tracebackhide__ = True
    actual = html.css_first(".overview-day-link")
    actual = get_normalized_text(actual)
    assert datestring == actual, (
        f"Incorrect datestring: found {actual} instead of {datestring}"
    )


def expect_values(
    s: TimePeriodOverview | None,
    dataset_count: int,
    footprint_count: int,
    time_range: Range,
    newest_creation_time: datetime,
    timeline_period: str,
    timeline_count: int,
    crses: set[str],
    size_bytes: int | None,
    region_dataset_counts: dict | None = None,
):
    assert s is not None, "No overview for dataset"
    __tracebackhide__ = True

    was_timeline_error = False
    was_regions_error = False
    try:
        assert s.dataset_count == dataset_count, "wrong dataset count"
        assert s.footprint_count == footprint_count, "wrong footprint count"
        if s.footprint_count is not None and s.footprint_count > 0:
            assert s.footprint_geometry is not None, (
                "No footprint, despite footprint count"
            )
            assert s.footprint_geometry.area > 0, "Empty footprint"

        assert s.time_range == time_range, "wrong dataset time range"
        assert s.newest_dataset_creation_time == default_utc(newest_creation_time), (
            "wrong newest dataset creation"
        )
        assert s.timeline_period == timeline_period, (
            f"Should be a {timeline_period}, not {s.timeline_period} timeline"
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
                    "null timeline_dataset_counts. "
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

        if region_dataset_counts is not None:
            was_regions_error = True
            if s.region_dataset_counts is None:
                if region_dataset_counts is not None:
                    raise AssertionError(
                        "No region counts found. "
                        f"Expected entry with {len(region_dataset_counts)} records."
                    )
            else:
                assert region_dataset_counts == s.region_dataset_counts
            was_regions_error = False
    except AssertionError:
        assert s.newest_dataset_creation_time is not None
        assert s.time_range is not None
        print(
            f"""Got:
        dataset_count {s.dataset_count}
        footprint_count {s.footprint_count}
        time range:
            - {s.time_range.begin.astimezone(timezone.utc)!r}
            - {s.time_range.end.astimezone(timezone.utc)!r}
        newest: {s.newest_dataset_creation_time.astimezone(timezone.utc)!r}
        crses: {s.crses!r}
        size_bytes: {s.size_bytes}
        timeline
            period: {s.timeline_period}
            dataset_counts: {None if s.timeline_dataset_counts is None else len(s.timeline_dataset_counts)}
        """
        )
        if was_timeline_error:
            print("timeline keys:")
            for day, count in s.timeline_dataset_counts.items():
                print(f"\t{day!r}: {count}")

        if was_regions_error:
            print("region keys:")
            for region, count in s.region_dataset_counts.items():
                print(f"\t{region!r}: {count}")
        raise


class DebugContext:
    """
    Add a message to be included if an assertion/validation
    error is thrown within this block of code.

    (for instance: which dataset was being tested in the list)

    This makes test failures a lot less obtuse!
    """

    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: AssertionError,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, (AssertionError, InvalidDocException)):
            _add_context(exc_val, self.msg)


def _add_context(e: AssertionError, context_message: str) -> None:
    """
    Append some extra information to an assertion error message .

    (Such as the url being tested, or the specific item that's failing)

    This is mildly distasteful, but catching and raising new exceptions
    makes misleading and very verbose output.

    We are adding extra information, not a different error.
    """
    args = list(e.args) or [""]
    separator = "\n\n==== Context ===="

    full_error = args[0]
    if isinstance(full_error, jsonschema.ValidationError):
        full_error = str(full_error)
    if isinstance(full_error, bytes):
        full_error = full_error.decode("utf-8")
    # Indent the message with a bullet "-" prefix
    context_message = indent(context_message, " " * 3)
    context_message = "-" + context_message[2:]

    if separator in full_error:
        # If there's already DebugContext, place the new message at the beginning.
        # (as unwinding happens backwards.)
        original, existing_context = full_error.split(separator)
        full_error = f"{original}{separator}\n{context_message}{existing_context}"
    else:
        full_error = f"{full_error}{separator}\n{context_message}"

    args[0] = full_error
    e.args = tuple(args)


def format_doc_diffs(left: dict, right: dict) -> Iterable[str]:
    """
    Get a human-readable list of differences in the given documents.

    Returns a list of lines to print.
    """
    doc_diffs = DeepDiff(left, right, significant_digits=6)
    out = []
    if doc_diffs:
        out.append("Documents differ:")
    else:
        out.append("Doc differs in minor float precision:")
        doc_diffs = DeepDiff(left, right)

    out.append(indent(pformat(doc_diffs), " " * 4))

    # If pytest verbose:
    out.extend(("Full output document: ", repr(left)))
    return out
