# -*- coding: utf-8 -*-
"""
Common global filters and util methods.
"""

import collections
import functools
import logging
from datetime import datetime

from dateutil import parser
from dateutil.relativedelta import relativedelta
from flask import Blueprint

from datacube.model import Range

_LOG = logging.getLogger(__name__)
bp = Blueprint("utils", __name__)

ACCEPTABLE_SEARCH_FIELDS = ["platform", "instrument", "product"]


@bp.app_template_filter("printable_time")
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter("query_value")
def _format_query_value(val):
    if isinstance(val, Range):
        return "{} to {}".format(
            _format_query_value(val.begin), _format_query_value(val.end)
        )
    if isinstance(val, datetime):
        return _format_datetime(val)
    return str(val)


@bp.app_template_filter("month_name")
def _format_month_name(val):
    ds = datetime(2016, int(val), 2)
    return ds.strftime("%b")


@bp.app_template_filter("max")
def _max_val(ls):
    return max(ls)


def parse_query(request):
    query = {}
    for field in ACCEPTABLE_SEARCH_FIELDS:
        if field in request:
            query[field] = request[field]

    to_time = parser.parse(request["before"]) if "before" in request else None
    from_time = parser.parse(request["after"]) if "after" in request else None

    # Default from/to values (a one month range)
    if not from_time and not to_time:
        to_time = datetime.now()
    if not to_time:
        to_time = from_time + relativedelta(months=1)
    if not from_time:
        from_time = to_time - relativedelta(months=1)

    query["time"] = Range(from_time, to_time)

    def range_dodge(val):
        if isinstance(val, list):
            return Range(val[0], val[1])
        else:
            return Range(val - 0.00005, val + 0.00005)

    if "lon" in request and "lat" in request:
        query["lon"] = range_dodge(request["lon"])
        query["lat"] = range_dodge(request["lat"])
    return query


def get_ordered_metadata(metadata_doc):
    def get_property_priority(ordered_properties, keyval):
        key, val = keyval
        if key not in ordered_properties:
            return 999
        return ordered_properties.index(key)

    # Give the document the same order as eo-datasets. It's far more readable (ID/names first, sources last etc.)
    ordered_metadata = collections.OrderedDict(
        sorted(
            metadata_doc.items(),
            key=functools.partial(get_property_priority, EODATASETS_PROPERTY_ORDER),
        )
    )
    ordered_metadata["lineage"] = collections.OrderedDict(
        sorted(
            ordered_metadata["lineage"].items(),
            key=functools.partial(
                get_property_priority, EODATASETS_LINEAGE_PROPERTY_ORDER
            ),
        )
    )

    if "source_datasets" in ordered_metadata["lineage"]:
        for type_, source_dataset_doc in ordered_metadata["lineage"][
            "source_datasets"
        ].items():
            ordered_metadata["lineage"]["source_datasets"][
                type_
            ] = get_ordered_metadata(source_dataset_doc)

    return ordered_metadata


EODATASETS_PROPERTY_ORDER = [
    "id",
    "ga_label",
    "ga_level",
    "product_type",
    "product_level",
    "product_doi",
    "creation_dt",
    "size_bytes",
    "checksum_path",
    "platform",
    "instrument",
    "format",
    "usgs",
    "rms_string",
    "acquisition",
    "extent",
    "grid_spatial",
    "gqa",
    "browse",
    "image",
    "lineage",
    "product_flags",
]
EODATASETS_LINEAGE_PROPERTY_ORDER = [
    "algorithm",
    "machine",
    "ancillary_quality",
    "ancillary",
    "source_datasets",
]
