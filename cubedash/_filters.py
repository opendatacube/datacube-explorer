"""
Common global filters for templates.
"""

import calendar
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import flask
import orjson
import structlog
from datacube.index.fields import Field
from datacube.model import Dataset, Product, Range
from flask import Blueprint
from markupsafe import Markup
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

_LOG = structlog.stdlib.get_logger()
bp = Blueprint("filters", __name__)


@bp.app_template_filter("printable_time")
def _format_datetime(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")


@bp.app_template_filter("metadata_center_time")
def _get_metadata_center_time(dataset):
    return utils.datetime_from_metadata(dataset)


@bp.app_template_filter("localised_metadata_center_time")
def _get_localised_metadata_center_time(date):
    return date.astimezone(ZoneInfo(_model.DEFAULT_GROUPING_TIMEZONE))


@bp.app_template_filter("printable_dataset")
def _dataset_label(dataset):
    label = utils.dataset_label(dataset)
    # If archived, strike out the label.
    if dataset.archived_time:
        return Markup("<del>{}</del>").format(label)
    return label


@bp.app_template_filter("torapidjson")
def _fast_tojson(obj):
    # FIXME: looks prone to XSS.
    return Markup(orjson.dumps(obj).decode("utf-8"))  # noqa: S704


@bp.app_template_filter("printable_data_size")
def sizeof_fmt(num, suffix="B") -> str:
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
    url = flask.url_for("pages.product_page", product_name=product_name)
    return Markup("<a href='{}' class='product-name'>{}</a>").format(url, product_name)


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
    return "" if not offset else _to_remote_url(offset, dataset.uri)


@bp.app_template_filter("resolve_remote_url")
def _to_remote_url(offset: str, base_uri: str | None = None):
    return utils.as_resolved_remote_url(base_uri, offset)


@bp.app_template_filter("all_values_none")
def _all_values_none(d: Mapping):
    return all(v is None for v in d.values())


@bp.app_template_filter("dataset_day_link")
def _dataset_day_link(dataset: Dataset, timezone=None):
    t = utils.datetime_from_metadata(dataset)
    if t is None:
        return "(unknown time)"
    if timezone:
        t = utils.default_utc(t).astimezone(timezone)
    url = flask.url_for(
        "pages.product_page",
        product_name=dataset.product.name,
        year=t.year,
        month=t.month,
        day=t.day,
    )
    return Markup("<a href='{}' class='overview-day-link'>{}{} {}</a>").format(
        url, t.day, _get_ordinal_suffix(t.day), t.strftime("%B %Y")
    )


@bp.app_template_filter("albers_area")
def _format_albers_area(shape: MultiPolygon):
    return Markup("{}km<sup>2</sup>").format(
        format(round(shape.area / 1_000_000), ",d")
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
def _maybe_format_css_class(val: str, prefix: str = "") -> str:
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
def _format_ordinal(val) -> str:
    return f"{val}{_get_ordinal_suffix(val)}"


def _get_ordinal_suffix(day):
    if 4 <= day <= 20 or 24 <= day <= 30:
        return "th"
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
def _product_license(product: Product):
    license_ = _utils.product_license(product)

    if license_ is None:
        return "-"

    if license_ in ("various", "proprietry"):
        return license_

    return Markup(
        "<a href='https://spdx.org/licenses/{}.html' class='spdx-license badge'>{}</a>"
    ).format(license_, license_)


@bp.app_template_filter("searchable_fields")
def _searchable_fields(product: Product) -> Iterable[tuple]:
    """Searchable field names for a product"""

    # No point searching fields that are fixed for this product
    # (eg: platform is always Landsat 7 on ls7_level1_scene)
    skippable_product_keys = {k for k, v in product.fields.items() if v is not None}

    return sorted(
        (key, field)
        for key, field in product.metadata_type.dataset_fields.items()
        if key != "product" and key not in skippable_product_keys and field.indexed  # type: ignore[attr-defined]
    )


@bp.app_template_filter("searchable_fields_keys")
def _searchable_fields_keys(product: Product):
    """List of keys of searchable field names for a product"""
    fields = _searchable_fields(product)
    return [k for k, _ in fields]


@bp.app_template_filter("is_numeric_field")
def _is_numeric_field(field: Field) -> bool:
    return field.type_name in NUMERIC_STEP_SIZE


@bp.app_template_filter("is_date_field")
def _is_date_field(field: Field) -> bool:
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

    now = datetime.now(timezone.utc)
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
    as_utc = actual_time.astimezone(timezone.utc)
    return Markup('<time datetime={} title="{}">{}</time>').format(
        as_utc.isoformat(), actual_time.strftime("%a, %d %b %Y %H:%M:%S%Z"), label
    )
