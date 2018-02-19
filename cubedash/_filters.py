"""
Common global filters for templates.
"""

from __future__ import absolute_import
from __future__ import division

import calendar
import logging
from datetime import datetime

from datacube.index.postgres._fields import PgField, IntDocField, DoubleDocField, NumericDocField, \
    RangeDocField, DateDocField

from datacube.model import Range, DatasetType
from dateutil import tz
from flask import Blueprint
from jinja2 import Markup, escape
from . import _utils as utils, _model as model

NUMERIC_FIELD_TYPES = (NumericDocField, IntDocField, DoubleDocField)

_LOG = logging.getLogger(__name__)
bp = Blueprint('filters', __name__)


@bp.app_template_filter('printable_time')
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter('printable_dataset')
def _dataset_label(dataset):
    label = utils.dataset_label(dataset)
    # If archived, strike out the label.
    if dataset.archived_time:
        return Markup("<del>{}</del>".format(escape(label)))

    return label


@bp.app_template_filter('dataset_geojson')
def _dataset_geojson(dataset):
    shape = model.dataset_shape(dataset)
    if not shape:
        return None

    return {
        'type': 'Feature',
        'geometry': shape.__geo_interface__,
        'properties': {
            'id': str(dataset.id),
            'label': utils.dataset_label(dataset),
            'start_time': dataset.time.begin.isoformat()
        }
    }


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
    return calendar.month_name[val]


@bp.app_template_filter('day_ordinal')
def _format_ordinal(val):
    return f"{val}{_get_ordinal_suffix(val)}"


def _get_ordinal_suffix(day):
    if 4 <= day <= 20 or 24 <= day <= 30:
        return "th"
    else:
        return ["st", "nd", "rd"][day % 10 - 1]


@bp.app_template_filter('days_in_month')
def day_range(year_month):
    year, month = year_month
    _, last_day = calendar.monthrange(year, month)
    return range(1, last_day + 1)


@bp.app_template_filter('max')
def _max_val(ls):
    return max(ls)


@bp.app_template_filter('searchable_fields')
def _searchable_fields(product: DatasetType):
    """Searchable field names for a product"""

    # No point searching fields that are fixed for this product
    # (eg: platform is always Landsat 7 on ls7_level1_scene)
    skippable_product_keys = [k for k, v in product.fields.items() if v is not None]

    return sorted(
        [(key, field) for key, field in product.metadata_type.dataset_fields.items()
         if key not in skippable_product_keys and key != 'product']
    )


@bp.app_template_filter('is_numeric_field')
def _is_numeric_field(field: PgField):
    if isinstance(field, RangeDocField):
        return field.FIELD_CLASS in NUMERIC_FIELD_TYPES
    else:
        return isinstance(field, NUMERIC_FIELD_TYPES)


@bp.app_template_filter('is_date_field')
def _is_date_field(field: PgField):
    if isinstance(field, RangeDocField):
        return field.FIELD_CLASS is DateDocField
    else:
        return isinstance(field, DateDocField)


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
            return _time("%d %s ago" % (period, singular if period == 1 else plural), dt)

    return _time(default, dt)


def _time(label: str, actual_time: datetime) -> Markup:
    return Markup(f"<time datetime={actual_time.isoformat()}"
                  f" title={actual_time.isoformat()}>"
                  f"{escape(label)}"
                  f"</time>")
