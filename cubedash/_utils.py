# -*- coding: utf-8 -*-
"""
Common global filters and util methods.
"""

from __future__ import division

import collections
import functools
import logging
import pathlib
from datetime import datetime

from dateutil import parser
from dateutil import tz
from dateutil.relativedelta import relativedelta
from flask import Blueprint
from jinja2 import Markup, escape

from datacube.model import Range

_LOG = logging.getLogger(__name__)
bp = Blueprint('utils', __name__)

ACCEPTABLE_SEARCH_FIELDS = ['platform', 'instrument', 'product']


@bp.app_template_filter('printable_time')
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter('printable_dataset')
def _dataset_label(dataset):
    label = _get_label(dataset)
    # If archived, strike out the label.
    if dataset.archived_time:
        return Markup("<del>{}</del>".format(escape(label)))
    else:
        return label


def _get_label(dataset):
    """
    :type dataset: datacube.model.Dataset
    :rtype: str
    """
    # Identify by label if they have one
    label = dataset.metadata.fields.get('label')
    if label is not None:
        return label
    # Otherwise by the file/folder name if there's a path.
    elif dataset.local_uri:
        p = pathlib.Path(dataset.local_uri)
        if p.name in ('ga-metadata.yaml', 'agdc-metadata.yaml'):
            return p.parent.name
        else:
            return p.name
    # TODO: Otherwise try to build a label from the available fields?
    return dataset.id


@bp.app_template_filter('query_value')
def _format_query_value(val):
    if isinstance(val, Range):
        return '{} to {}'.format(_format_query_value(val.begin), _format_query_value(val.end))
    if isinstance(val, datetime):
        return _format_datetime(val)
    return str(val)


@bp.app_template_filter('month_name')
def _format_month_name(val):
    ds = datetime(2016, int(val), 2)
    return ds.strftime("%b")


@bp.app_template_filter('max')
def _max_val(ls):
    return max(ls)


@bp.app_template_filter('timesince')
def timesince(dt, default="just now"):
    """
    Returns string representing "time since" e.g.
    3 days ago, 5 hours ago etc.

    http://flask.pocoo.org/snippets/33/
    """

    now = datetime.utcnow().replace(tzinfo=tz.tzutc())
    diff = now - dt

    periods = (
        (diff.days // 365, "year", "years"),
        (diff.days // 30, "month", "months"),
        (diff.days // 7, "week", "weeks"),
        (diff.days, "day", "days"),
        (diff.seconds // 3600, "hour", "hours"),
        (diff.seconds // 60, "minute", "minutes"),
        (diff.seconds, "second", "seconds"),
    )

    for period, singular, plural in periods:

        if period:
            return "%d %s ago" % (period, singular if period == 1 else plural)

    return default


def parse_query(request):
    query = {}
    for field in ACCEPTABLE_SEARCH_FIELDS:
        if field in request:
            query[field] = request[field]

    to_time = parser.parse(request['before']) if 'before' in request else None
    from_time = parser.parse(request['after']) if 'after' in request else None

    # Default from/to values (a one month range)
    if not from_time and not to_time:
        to_time = datetime.now()
    if not to_time:
        to_time = from_time + relativedelta(months=1)
    if not from_time:
        from_time = to_time - relativedelta(months=1)

    query['time'] = Range(from_time, to_time)

    def range_dodge(val):
        if isinstance(val, list):
            return Range(val[0], val[1])
        else:
            return Range(val - 0.00005, val + 0.00005)

    if 'lon' in request and 'lat' in request:
        query['lon'] = range_dodge(request['lon'])
        query['lat'] = range_dodge(request['lat'])
    return query


def get_ordered_metadata(metadata_doc):
    def get_property_priority(ordered_properties, keyval):
        key, val = keyval
        if key not in ordered_properties:
            return 999
        return ordered_properties.index(key)

    # Give the document the same order as eo-datasets. It's far more readable (ID/names first, sources last etc.)
    ordered_metadata = collections.OrderedDict(
        sorted(metadata_doc.items(),
               key=functools.partial(get_property_priority, EODATASETS_PROPERTY_ORDER))
    )
    ordered_metadata['lineage'] = collections.OrderedDict(
        sorted(ordered_metadata['lineage'].items(),
               key=functools.partial(get_property_priority, EODATASETS_LINEAGE_PROPERTY_ORDER))
    )

    if 'source_datasets' in ordered_metadata['lineage']:
        for type_, source_dataset_doc in ordered_metadata['lineage']['source_datasets'].items():
            ordered_metadata['lineage']['source_datasets'][type_] = get_ordered_metadata(source_dataset_doc)

    return ordered_metadata


EODATASETS_PROPERTY_ORDER = ['id', 'ga_label', 'ga_level', 'product_type', 'product_level', 'product_doi',
                             'creation_dt', 'size_bytes', 'checksum_path', 'platform', 'instrument', 'format', 'usgs',
                             'rms_string', 'acquisition', 'extent', 'grid_spatial', 'gqa', 'browse', 'image', 'lineage',
                             'product_flags']
EODATASETS_LINEAGE_PROPERTY_ORDER = ['algorithm', 'machine', 'ancillary_quality', 'ancillary', 'source_datasets']
