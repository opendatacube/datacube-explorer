# -*- coding: utf-8 -*-
"""
Common global filters and util methods.
"""

from __future__ import absolute_import, division

import collections
import functools
from collections import defaultdict
from datetime import datetime

from dateutil.relativedelta import relativedelta
from werkzeug.datastructures import MultiDict

from datacube.index.postgres._fields import PgDocField, RangeDocField
from datacube.model import DatasetType, Range

DEFAULT_PLATFORM_END_DATE = {
    "LANDSAT_8": datetime.now() - relativedelta(months=2),
    "LANDSAT_7": datetime.now() - relativedelta(months=2),
    "LANDSAT_5": datetime(2011, 11, 30),
}


def group_field_names(request: dict) -> dict:
    """
    In a request, a dash separates field names from a classifier (eg: begin/end).

    Group the query classifiers by field names.

    >>> group_field_names({'lat-begin': '1', 'lat-end': '2', 'orbit': 3})
    {'lat': {'begin': 1, 'end': 2}, 'orbit': {'val': 3}}
    """
    out = defaultdict(dict)

    for field_expr, val in request.items():
        comps = field_expr.split("-")
        field_name = comps[0]

        if len(comps) == 1:
            constraint = "val"
        elif len(comps) == 2:
            constraint = comps[1]
        else:
            raise ValueError("Corrupt field name " + field_expr)

        # Skip empty values
        if val is None or val == "":
            continue

        out[field_name][constraint] = val
    return dict(out)


def query_to_search(request: MultiDict, product: DatasetType) -> dict:
    args = _parse_url_query_args(request, product)
    args = _ensure_minimum_search_filters(args, product)
    return args


def _parse_url_query_args(request: MultiDict, product: DatasetType) -> dict:
    """
    Convert search arguments from url query args into datacube index search parameters
    """
    query = {}

    field_groups = group_field_names(request)

    for field_name, field_vals in field_groups.items():
        field = product.metadata_type.dataset_fields.get(field_name)
        if not field:
            raise ValueError("No field %r for product %s" % (field_name, product.name))

        if isinstance(field, RangeDocField):
            parser = field.lower.parse_value
        elif isinstance(field, PgDocField):
            parser = field.parse_value
        else:
            parser = lambda a: a

        if "val" in field_vals:
            query[field_name] = parser(field_vals["val"])
        elif "begin" in field_vals or "end" in field_vals:
            begin, end = field_vals.get("begin"), field_vals.get("end")
            query[field_name] = Range(
                parser(begin) if begin else None, parser(end) if end else None
            )
        else:
            raise ValueError("Unknown field classifier: %r" % field_vals)

    return query


def _ensure_minimum_search_filters(in_query: dict, product: DatasetType):
    """
    At a minimum, the query should filter by the current product and a time period.

    (without these filters, the query would return all datasets in the datacube)
    """
    out_query = {"product": product.name, **in_query}

    time = in_query.get("time", Range(None, None))
    from_time, to_time = time

    # Default from/to values (a one month range)
    if not from_time and not to_time:
        platform_name = product.fields.get("platform")
        if platform_name in DEFAULT_PLATFORM_END_DATE:
            to_time = DEFAULT_PLATFORM_END_DATE[platform_name]
        else:
            to_time = datetime.now()
    if not to_time:
        to_time = from_time + relativedelta(months=1)
    if not from_time:
        from_time = to_time - relativedelta(months=1)

    out_query["time"] = Range(from_time, to_time)

    return out_query


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
