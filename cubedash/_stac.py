import json
import logging
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from flask import Blueprint, abort, request, url_for

from cubedash.summary._stores import DatasetItem
from datacube.model import Dataset, DatasetType
from datacube.utils import parse_time
from datacube.utils.uris import uri_resolve

from . import _model, _utils
from ._utils import default_utc as utc

_LOG = logging.getLogger(__name__)
bp = Blueprint("stac", __name__)

PAGE_SIZE_LIMIT = 1000
DEFAULT_PAGE_SIZE = 20

_STAC_DEFAULTS = dict(stac_version="0.6.0")

# TODO: move to config
ENDPOINT_ID = "dea"
ENDPOINT_TITLE = ""
ENDPOINT_DESCRIPTION = ""


@bp.route("/stac")
def root():
    """
    Links to product catalogs.
    """
    return _utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=ENDPOINT_ID,
            title=ENDPOINT_TITLE,
            description=ENDPOINT_DESCRIPTION,
            links=[
                *(
                    dict(
                        rel="child",
                        title=product.name,
                        description=product.definition.get("description"),
                        href=url_for(
                            ".collection", product_name=product.name, _external=True
                        ),
                    )
                    for product, product_summary in _model.get_products_with_summaries()
                ),
                dict(rel="self", href=request.url),
            ],
        )
    )


@bp.route("/stac/search", methods=["GET", "POST"])
def stac_search():
    if request.method == "GET":
        bbox = request.args.get("bbox")
        if bbox:
            bbox = json.loads(bbox)
        time = request.args.get("time")
        product_name = request.args.get("product")
        limit = request.args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
        offset = request.args.get("offset", default=0, type=int)
    else:
        req_data = request.get_json()
        bbox = req_data.get("bbox")
        time = req_data.get("time")
        product_name = req_data.get("product")
        limit = req_data.get("limit") or DEFAULT_PAGE_SIZE
        offset = req_data.get("offset") or 0

    if limit > PAGE_SIZE_LIMIT:
        abort(
            400,
            f"Max page size is {PAGE_SIZE_LIMIT}. "
            f"Use the next links instead of a large limit.",
        )

    if bbox is not None and len(bbox) != 4:
        abort(400, "Expected bbox of size 4. [min lon, min lat, max long, max lat]")

    if time is not None:
        time = _parse_time_range(time)

    def next_page_url(next_offset):
        return url_for(
            ".stac_search",
            product=product_name,
            bbox="[{},{},{},{}]".format(*bbox) if bbox else None,
            time=_unparse_time_range(time) if time else None,
            limit=limit,
            offset=next_offset,
        )

    return _utils.as_json(
        search_stac_items(
            product_name=product_name,
            bbox=bbox,
            time=time,
            limit=limit,
            offset=offset,
            get_next_url=next_page_url,
        )
    )


def search_stac_items(
    get_next_url: Callable[[int], str],
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
    product_name: Optional[str] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    time: Optional[Tuple[datetime, datetime]] = None,
):
    """
    Returns a GeoJson FeatureCollection corresponding to given parameters for
    a set of datasets returned by datacube.
    """
    offset = offset or 0
    items = list(
        _model.STORE.search_items(
            product_name=product_name,
            time=time,
            bbox=bbox,
            limit=limit + 1,
            offset=offset,
            full_dataset=True,
        )
    )

    result = dict(
        type="FeatureCollection",
        features=[as_stac_item(f) for f in items[:limit]],
        meta=dict(page=offset // limit, limit=limit),
        links=[],
    )

    there_are_more = len(items) == limit + 1

    if there_are_more:
        result["links"].append(dict(rel="next", href=get_next_url(offset + limit)))

    return result


@bp.route("/collections/<product_name>")
def collection(product_name: str):
    summary = _model.get_product_summary(product_name)
    dataset_type = _model.STORE.get_dataset_type(product_name)
    all_time_summary = _model.get_time_summary(product_name)

    summary_props = {}
    if summary and summary.time_earliest:
        begin, end = utc(summary.time_earliest), utc(summary.time_latest)
        extent = {"temporal": [begin, end]}
        footprint = all_time_summary.footprint_wrs84
        if footprint:
            extent["spatial"] = footprint.bounds

        summary_props["extent"] = extent
    return _utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            id=summary.name,
            title=summary.name,
            description=dataset_type.definition.get("description"),
            properties=dict(_build_properties(dataset_type)),
            providers=[],
            **summary_props,
            links=[
                dict(
                    rel="items",
                    href=url_for(
                        ".collection_items", product_name=product_name, _external=True
                    ),
                )
            ],
        )
    )


@bp.route("/collections/<product_name>/items")
def collection_items(product_name: str):
    def next_url(offset):
        return url_for(".collection_items", product_name=product_name, offset=offset)

    all_time_summary = _model.get_time_summary(product_name)
    if not all_time_summary:
        abort(404, "Product not yet summarised")

    feature_collection = search_stac_items(
        product_name=product_name, limit=PAGE_SIZE_LIMIT, get_next_url=next_url
    )

    # Maybe we shouldn't include "found" as it prevents some future optimisation?
    feature_collection["meta"]["found"] = all_time_summary.dataset_count

    return _utils.as_json(feature_collection)


@bp.route("/collections/<product_name>/items/<dataset_id>")
def item(product_name, dataset_id):
    dataset = _model.STORE.get_item(dataset_id)
    if not dataset:
        abort(404, "No such dataset")

    actual_product_name = dataset.product_name
    if product_name != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're jerks)
        actual_url = url_for(
            ".item", product_name=product_name, dataset_id=dataset_id, _external=True
        )
        abort(
            404,
            f"No such dataset in collection.\n"
            f"Perhaps you meant collection {actual_product_name}: {actual_url})",
        )

    return _utils.as_json(as_stac_item(dataset))


def pick_remote_uri(uris: Iterable[str]):
    # Return first uri with a remote path (newer paths come first)
    for uri in uris:
        scheme, *_ = uri.split(":")
        if scheme in ("https", "http", "ftp", "s3", "gfs"):
            return uri

    return None


def _parse_time_range(time: str) -> Tuple[datetime, datetime]:
    """
    >>> _parse_time_range('1986-04-16T01:12:16/2097-05-10T00:24:21')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(2097, 5, 10, 0, 24, 21))
    >>> _parse_time_range('1986-04-16T01:12:16')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(1986, 4, 16, 1, 12, 17))
    >>> _parse_time_range('1986-04-16')
    (datetime.datetime(1986, 4, 16, 0, 0), datetime.datetime(1986, 4, 17, 0, 0))
    """
    time_period = time.split("/")
    if len(time_period) == 2:
        return parse_time(time_period[0]), parse_time(time_period[1])
    elif len(time_period) == 1:
        t: datetime = parse_time(time_period[0])
        if t.time() == dt_time():
            return t, t + timedelta(days=1)
        else:
            return t, t + timedelta(seconds=1)


def _unparse_time_range(time: Tuple[datetime, datetime]) -> str:
    """
    >>> _unparse_time_range((
    ...     datetime(1986, 4, 16, 1, 12, 16),
    ...     datetime(2097, 5, 10, 0, 24, 21)
    ... ))
    '1986-04-16T01:12:16/2097-05-10T00:24:21'
    """
    start_time, end_time = time
    return f"{start_time.isoformat()}/{end_time.isoformat()}"


def as_stac_item(dataset: DatasetItem):
    """
    Returns a dict corresponding to a stac item
    """
    ds = dataset.odc_dataset
    item_doc = dict(
        id=dataset.dataset_id,
        type="Feature",
        bbox=dataset.bbox,
        geometry=dataset.geom_geojson,
        properties={
            "datetime": dataset.center_time,
            "odc:product": dataset.product_name,
            "odc:creation-time": dataset.creation_time,
            "cubedash:region_code": dataset.region_code,
        },
        assets=_stac_item_assets(ds),
        links=[
            {
                "rel": "self",
                "href": url_for(
                    ".item",
                    product_name=dataset.product_name,
                    dataset_id=dataset.dataset_id,
                ),
            },
            {
                "rel": "parent",
                "href": url_for(".collection", product_name=dataset.product_name),
            },
        ],
    )

    # If the dataset has a real start/end time, add it.
    time = ds.time
    if time.begin < time.end:
        # datetime range extension propeosal (dtr):
        # https://github.com/radiantearth/stac-spec/tree/master/extensions/datetime-range
        item_doc["properties"]["dtr:start_datetime"] = utc(time.begin)
        item_doc["properties"]["dtr:end_datetime"] = utc(time.end)

    return item_doc


def _stac_item_assets(ds):
    base_uri = pick_remote_uri(ds.uris) or ds.local_uri

    def measurement_to_asset(name: str, data: Dict) -> Dict:
        return {"href": uri_resolve(base_uri, data.get("path"))}

    # TODO: measurements should actually map to "eo:bands", right?
    assets = {
        name: measurement_to_asset(name, data) for name, data in ds.measurements.items()
    }

    # Add an "odc:location" field with our base uri if we have one.
    if ds.uris:
        locs = {"href": ds.uris[0]}
        remaining = ds.uris[1:]
        if remaining:
            locs["odc:secondary_hrefs"] = remaining
        assets[f"odc:location"] = locs
    return assets


def field_platform(value):
    return "eo:platform", value.lower().replace("_", "-")


def field_instrument(value):
    return "eo:instrument", value


def field_bands(value: List[Dict]):
    return "eo:bands", [dict(name=v["name"]) for v in value]


def field_path_row(value):
    # eo:row	"135"
    # eo:column	"044"
    pass


# Other Property examples:
# collection	"landsat-8-l1"
# eo:gsd	15
# eo:platform	"landsat-8"
# eo:instrument	"OLI_TIRS"
# eo:off_nadir	0
# datetime	"2019-02-12T19:26:08.449265+00:00"
# eo:sun_azimuth	-172.29462212
# eo:sun_elevation	-6.62176054
# eo:cloud_cover	-1
# eo:row	"135"
# eo:column	"044"
# landsat:product_id	"LC08_L1GT_044135_20190212_20190212_01_RT"
# landsat:scene_id	"LC80441352019043LGN00"
# landsat:processing_level	"L1GT"
# landsat:tier	"RT"

_STAC_PROPERTY_MAP = {
    "platform": field_platform,
    "instrument": field_instrument,
    "measurements": field_bands,
}


def _build_properties(dt: DatasetType):
    for key, val in dt.metadata.fields.items():
        if val is None:
            continue
        converter = _STAC_PROPERTY_MAP.get(key)
        if converter:
            yield converter(val)
