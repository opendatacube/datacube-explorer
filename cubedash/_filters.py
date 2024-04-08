"""
Common global filters for templates.
"""

import calendar
import logging
from datetime import datetime
from typing import Mapping
from urllib.parse import quote_plus

import flask
import pytz
from datacube.index.fields import Field
from datacube.model import Dataset, DatasetType, Range
from dateutil import tz
from flask import Blueprint
from markupsafe import Markup, escape
from orjson import orjson
from shapely.geometry import MultiPolygon

from . import _model, _utils
from . import _utils as utils

# How far to step the number when the user hits up/down.
NUMERIC_STEP_SIZE = {
    "numeric-range": 0.001,
    "double-range": 0.001,
    "integer-range": 1,
    "numeric": 0.001,
    "double": 0.001,
    "integer": 1,
}

CROSS_SYMBOL = Markup('<i class="fa fa-times" aria-label="x"></i>')

_LOG = logging.getLogger(__name__)
bp = Blueprint("filters", __name__)


@bp.app_template_filter("printable_time")
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter("metadata_center_time")
def _get_metadata_center_time(dataset):
    return utils.center_time_from_metadata(dataset)


@bp.app_template_filter("localised_metadata_center_time")
def _get_localised_metadata_center_time(date):
    return date.astimezone(pytz.timezone(_model.DEFAULT_GROUPING_TIMEZONE))


@bp.app_template_filter("printable_dataset")
def _dataset_label(dataset):
    label = utils.dataset_label(dataset)
    # If archived, strike out the label.
    if dataset.archived_time:
        return Markup(f"<del>{escape(label)}</del>")

    return label


@bp.app_template_filter("torapidjson")
def _fast_tojson(obj):
    return Markup(orjson.dumps(obj).decode("utf-8"))


@bp.app_template_filter("printable_data_size")
def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


@bp.app_template_filter("percent")
def percent_fmt(val, total, show_zero=False):
    if val is None:
        return ""
    if val == 0 and not show_zero:
        return ""
    if val == total:
        return CROSS_SYMBOL
    o = 100 * (val / total)
    return f"{o:.2f}%"


@bp.app_template_filter("dataset_geojson")
def _dataset_geojson(dataset):
    shape, valid_extent = utils.dataset_shape(dataset)
    if not shape:
        return None

    return {
        "type": "Feature",
        "geometry": shape.__geo_interface__,
        "properties": {
            "id": str(dataset.id),
            "label": utils.dataset_label(dataset),
            "valid_extent": valid_extent,
            "start_time": dataset.time.begin.isoformat(),
        },
    }


@bp.app_template_filter("product_link")
def _product_link(product_name):
    url = flask.url_for("product_page", product_name=product_name)
    return Markup(f"<a href='{url}' class='product-name'>{product_name}</a>")


@bp.app_template_filter("dataset_created")
def _dataset_created(dataset: Dataset):
    return utils.dataset_created(dataset)


@bp.app_template_filter("dataset_file_paths")
def _dataset_file_paths(dataset: Dataset):
    return utils.get_dataset_file_offsets(dataset)


@bp.app_template_filter("dataset_thumbnail_url")
def _dataset_thumbnail_url(dataset: Dataset):
    file_paths = _dataset_file_paths(dataset)
    offset = file_paths.get("thumbnail:nbart") or file_paths.get("thumbnail")
    return "" if not offset else _to_remote_url(offset, dataset.uris[0])


@bp.app_template_filter("resolve_remote_url")
def _to_remote_url(offset: str, base_uri: str = None):
    return utils.as_resolved_remote_url(base_uri, offset)


@bp.app_template_filter("all_values_none")
def _all_values_none(d: Mapping):
    return all(v is None for v in d.values())


@bp.app_template_filter("dataset_day_link")
def _dataset_day_link(dataset: Dataset, timezone=None):
    t = utils.center_time_from_metadata(dataset)
    if t is None:
        return "(unknown time)"
    if timezone:
        t = utils.default_utc(t).astimezone(timezone)
    url = flask.url_for(
        "product_page",
        product_name=dataset.type.name,
        year=t.year,
        month=t.month,
        day=t.day,
    )
    return Markup(
        f"<a href='{url}' class='overview-day-link'>"
        f"{t.day}{_get_ordinal_suffix(t.day)} "
        f"{t.strftime('%B %Y')}"
        f"</a>"
    )


@bp.app_template_filter("albers_area")
def _format_albers_area(shape: MultiPolygon):
    return Markup(
        "{}km<sup>2</sup>".format(format(round(shape.area / 1_000_000), ",d"))
    )


_NULL_VALUE = Markup('<span class="null-value" title="Unspecified">•</span>')


@bp.app_template_filter("query_value")
def _format_query_value(val):
    if isinstance(val, Range):
        return f"{_format_query_value(val.begin)} to {_format_query_value(val.end)}"
    if isinstance(val, datetime):
        return _format_datetime(val)
    if val is None:
        return _NULL_VALUE
    if isinstance(val, float):
        return round(val, 3)
    return str(val)


@bp.app_template_filter("maybe_to_css_class_name")
def _maybe_format_css_class(val: str, prefix: str = ""):
    """
    Create a CSS class name for the given string if it is safe to do so.

    Otherwise return nothing
    """
    if val.replace("-", "_").isidentifier():
        return f"{prefix}{val}"
    return ""


@bp.app_template_filter("month_name")
def _format_month_name(val):
    return calendar.month_name[val]


@bp.app_template_filter("day_ordinal")
def _format_ordinal(val):
    return f"{val}{_get_ordinal_suffix(val)}"


def _get_ordinal_suffix(day):
    if 4 <= day <= 20 or 24 <= day <= 30:
        return "th"
    else:
        return ["st", "nd", "rd"][day % 10 - 1]


@bp.app_template_filter("days_in_month")
def day_range(year_month):
    year, month = year_month
    _, last_day = calendar.monthrange(year, month)
    return range(1, last_day + 1)


@bp.app_template_filter("max")
def _max_val(ls):
    return max(ls)


@bp.app_template_filter("product_license_link")
def _product_license(product: DatasetType):
    license_ = _utils.product_license(product)

    if license_ is None:
        return "-"

    if license_ in ("various", "proprietry"):
        return license_

    return Markup(
        f"<a href='https://spdx.org/licenses/"
        f"{quote_plus(license_)}.html' "
        f"class='spdx-license badge'>{license_}"
        f"</a>"
    )


@bp.app_template_filter("searchable_fields")
def _searchable_fields(product: DatasetType):
    """Searchable field names for a product"""

    # No point searching fields that are fixed for this product
    # (eg: platform is always Landsat 7 on ls7_level1_scene)
    skippable_product_keys = [k for k, v in product.fields.items() if v is not None]

    return sorted(
        (key, field)
        for key, field in product.metadata_type.dataset_fields.items()
        if key not in skippable_product_keys and key != "product"
    )


@bp.app_template_filter("searchable_fields_keys")
def _searchable_fields_keys(product: DatasetType):
    """List of keys of searchable field names for a product"""
    fields = _searchable_fields(product)
    return [k for k, _ in fields]


@bp.app_template_filter("is_numeric_field")
def _is_numeric_field(field: Field):
    return field.type_name in NUMERIC_STEP_SIZE


@bp.app_template_filter("is_date_field")
def _is_date_field(field: Field):
    return field.type_name in ("datetime", "datetime-range")


@bp.app_template_filter("field_step_size")
def _field_step(field: Field):
    return NUMERIC_STEP_SIZE.get(field.type_name, 1)


@bp.app_template_filter("only_alnum")
def only_alphanumeric(s):
    return _utils.only_alphanumeric(s)


@bp.app_template_filter("timesince")
def timesince(dt, default="just now"):
    """
    Returns string representing "time since" e.g.
    3 days ago, 5 hours ago etc.

    http://flask.pocoo.org/snippets/33/
    """
    if dt is None:
        return "an unrecorded time ago"

    now = datetime.utcnow().replace(tzinfo=tz.tzutc())
    diff = now - utils.default_utc(dt)

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
            return _time(f"{period:d} {singular if period == 1 else plural} ago", dt)

    return _time(default, dt)


def _time(label: str, actual_time: datetime) -> Markup:
    as_utc = actual_time.astimezone(tz.tzutc())
    return Markup(
        f"<time datetime={as_utc.isoformat()}"
        f' title="{actual_time.strftime("%a, %d %b %Y %H:%M:%S%Z")}">'
        f"{escape(label)}"
        f"</time>"
    )
