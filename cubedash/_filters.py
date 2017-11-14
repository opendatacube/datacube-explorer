"""
Common global filters for templates.
"""

from __future__ import absolute_import
from __future__ import division

import logging
import pathlib
from datetime import datetime

from datacube.index.postgres._fields import PgField, IntDocField, DoubleDocField, NumericDocField, RangeDocField

from datacube.model import Range
from dateutil import tz
from flask import Blueprint
from jinja2 import Markup, escape

NUMERIC_FIELD_TYPES = (NumericDocField, IntDocField, DoubleDocField)

_LOG = logging.getLogger(__name__)
bp = Blueprint('filters', __name__)


@bp.app_template_filter('printable_time')
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter('printable_dataset')
def _dataset_label(dataset):
    label = _get_label(dataset)
    # If archived, strike out the label.
    if dataset.archived_time:
        return Markup("<del>{}</del>".format(escape(label)))

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

        return p.name
    # TODO: Otherwise try to build a label from the available fields?
    return dataset.id


@bp.app_template_filter('query_value')
def _format_query_value(val):
    if isinstance(val, Range):
        return '{} to {}'.format(_format_query_value(val.begin), _format_query_value(val.end))
    if isinstance(val, datetime):
        return _format_datetime(val)
    if val is None:
        return '•'
    return str(val)


@bp.app_template_filter('month_name')
def _format_month_name(val):
    ds = datetime(2016, int(val), 2)
    return ds.strftime("%b")


@bp.app_template_filter('max')
def _max_val(ls):
    return max(ls)


@bp.app_template_filter('is_numeric_field')
def _is_numeric_field(field: PgField):
    if isinstance(field, RangeDocField):
        return field.FIELD_CLASS in NUMERIC_FIELD_TYPES
    else:
        return isinstance(field, NUMERIC_FIELD_TYPES)


@bp.app_template_filter('field_step_size')
def _field_step(field: PgField):
    if isinstance(field, RangeDocField):
        field = field.FIELD_CLASS

    return {
        IntDocField: 1,
        NumericDocField: 0.001,
        DoubleDocField: 0.001,
    }.get(field.__class__, 1)


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
