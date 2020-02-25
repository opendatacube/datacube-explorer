import json
import logging
from collections import defaultdict
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Sequence, Tuple
from urllib.parse import urljoin

from dateutil.tz import tz

import flask
from cubedash.summary._stores import DatasetItem
from datacube.model import Dataset, Range
from datacube.utils import DocReader, parse_time
from flask import abort, request

from . import _model, _utils

_LOG = logging.getLogger(__name__)
bp = flask.Blueprint("stac", __name__)

PAGE_SIZE_LIMIT = _model.app.config.get("STAC_PAGE_SIZE_LIMIT", 1000)
DEFAULT_PAGE_SIZE = _model.app.config.get("STAC_DEFAULT_PAGE_SIZE", 20)
# Should we force all URLs to include the full hostname?
FORCE_ABSOLUTE_LINKS = _model.app.config.get("STAC_ABSOLUTE_HREFS", True)

_STAC_DEFAULTS = dict(stac_version="0.6.0")


def url_for(*args, **kwargs):
    if FORCE_ABSOLUTE_LINKS:
        kwargs["_external"] = True
    return flask.url_for(*args, **kwargs)


def _endpoint_params() -> Dict:
    config = _model.app.config
    o = dict(
        id=config.get("STAC_ENDPOINT_ID", "odc-explorer"),
        title=config.get("STAC_ENDPOINT_TITLE", "Default ODC Explorer instance"),
    )
    description = config.get("STAC_ENDPOINT_DESCRIPTION")
    if description:
        o["description"] = description
    return o


def utc(d: datetime):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


@bp.route("/stac")
def root():
    """
    The root stac page links to each collection (product) catalog
    """
    return _utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            **_endpoint_params(),
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
    """
    Search api for stac items.
    """
    if request.method == "GET":
        bbox = request.args.get("bbox")
        if bbox:
            bbox = json.loads(bbox)
        time = request.args.get("time")
        product_name = request.args.get("product")
        limit = request.args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
        offset = request.args.get("_o", default=0, type=int)
    else:
        req_data = request.get_json()
        bbox = req_data.get("bbox")
        time = req_data.get("time")
        product_name = req_data.get("product")
        limit = req_data.get("limit") or DEFAULT_PAGE_SIZE
        offset = req_data.get("_o") or 0

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
            _o=next_offset,
        )

    return _utils.as_geojson(
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
) -> Dict:
    """
    Perform a search, returning a FeatureCollection of stac Item results.

    :param get_next_url: A function that calculates a page url for the given offset.
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
    """
    Overview of a WFS Collection (a datacube product)
    """
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
    return _utils.as_geojson(
        dict(
            **_STAC_DEFAULTS,
            id=summary.name,
            title=summary.name,
            description=dataset_type.definition.get("description"),
            properties=dict(_build_properties(dataset_type.metadata)),
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
    """
    A geojson FeatureCollection of all items in a collection/product.

    (with paging)
    """

    def next_url(offset):
        return url_for(".collection_items", product_name=product_name, _o=offset)

    all_time_summary = _model.get_time_summary(product_name)
    if not all_time_summary:
        abort(404, "Product not yet summarised")

    feature_collection = search_stac_items(
        product_name=product_name,
        limit=PAGE_SIZE_LIMIT,
        get_next_url=next_url,
        offset=request.args.get("_o", default=0, type=int),
    )

    # Maybe we shouldn't include "found" as it prevents some future optimisation?
    feature_collection["meta"]["found"] = all_time_summary.dataset_count

    return _utils.as_geojson(feature_collection)


@bp.route("/collections/<product_name>/items/<dataset_id>")
def item(product_name, dataset_id):
    dataset = _model.STORE.get_item(dataset_id)
    if not dataset:
        abort(404, "No such dataset")

    actual_product_name = dataset.product_name
    if product_name != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're unkind)
        actual_url = url_for(
            ".item", product_name=product_name, dataset_id=dataset_id, _external=True
        )
        abort(
            404,
            f"No such dataset in collection.\n"
            f"Perhaps you meant collection {actual_product_name}: {actual_url})",
        )

    return _utils.as_geojson(as_stac_item(dataset))


def _pick_remote_uri(uris: Sequence[str]) -> Optional[int]:
    """
    Return the offset of the first uri with a remote path, if any.
    """
    for i, uri in enumerate(uris):
        scheme, *_ = uri.split(":")
        if scheme in ("https", "http", "ftp", "s3", "gfs"):
            return i
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
    Get a dict corresponding to a stac item
    """
    ds = dataset.odc_dataset
    item_doc = dict(
        id=dataset.dataset_id,
        type="Feature",
        bbox=dataset.bbox,
        geometry=dataset.geom_geojson,
        properties={
            "datetime": utc(dataset.center_time),
            **dict(_build_properties(dataset.odc_dataset.metadata)),
            "odc:product": dataset.product_name,
            "odc:processing_datetime": utc(dataset.creation_time),
            "cubedash:region_code": dataset.region_code
        },
        assets=dict(_stac_item_assets(ds)),
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
            {
                "rel": "alternative",
                "type": "text/html",
                "href": url_for("dataset.dataset_page", id_=dataset.dataset_id),
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


def _stac_item_assets(ds: Dataset) -> Iterable[Tuple[str, Dict]]:
    """
    A list of assets is the list of files for the dataset.

    We group bands/measurements together if they're in the same file (eg. .nc)
    """
    # The main uri is what we use for expanding all relative paths.
    main_uri = None
    uris = list(ds.uris)
    if uris:
        # If one of the uris is a remote uri, make it the main one.
        main_uri = uris.pop(_pick_remote_uri(ds.uris) or 0)

    # Group measurements that have the same path, they should be listed as one asset.
    assets_by_path = defaultdict(dict)

    for name, data in ds.measurements.items():
        path = uri_resolve(main_uri, data.get("path") or None)
        if not path:
            continue

        asset = assets_by_path.get(path)
        if asset:
            asset["eo:bands"].append(name)
        else:
            assets_by_path[path] = {"eo:bands": [name], "href": path}

    # Ensure there's an asset for the main uri/location.
    if main_uri:
        base_asset = assets_by_path.get(main_uri)
        if not base_asset:
            base_asset = {"href": main_uri}
            assets_by_path[main_uri] = base_asset

        base_asset["odc:secondary_hrefs"] = uris

    # Now how do we name our assets?
    for asset in assets_by_path.values():
        # If there's one band, name it by that.
        bands = asset.get("eo:bands")
        if bands and len(bands) == 1:
            (asset_name,) = bands
        elif asset["href"] == main_uri:
            asset_name = "location"
        else:
            # Otherwise extract the "stem" from the filename.
            _, _, filename = asset["href"].rpartition("/")
            asset_name, _, _ = filename.partition(".")

        yield asset_name, asset


def field_platform(key, value):
    yield "eo:platform", value.lower().replace("_", "-")


def field_instrument(key, value):
    yield "eo:instrument", value


def field_bands(key, value: Dict):
    yield "eo:bands", [dict(name=k, **v) for k, v in value.items()]


def field_path_row(key, value):
    # Path/Row fields are ranges in datacube but 99% of the time
    # they are a single value
    # (they are ranges in telemetry products)
    # Stac doesn't accept a range here, so we'll skip it in those products,
    # but we can handle the 99% case when lower==higher.
    if key == "sat_path":
        kind = "column"
    elif key == "sat_row":
        kind = "row"
    else:
        raise ValueError(f"Path/row kind {repr(key)}")

    # If there's only one value in the range, return it.
    if isinstance(value, Range):
        if value.end is None or value.begin == value.end:
            # Standard stac
            yield f"eo:{kind}", str(value.begin)
        else:
            # Our questionable output. Only present in telemetry products?
            yield f"odc:{key}", f"{value.begin}/{value.end}"


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
    # "measurements": field_bands,
    "sat_path": field_path_row,
    "sat_row": field_path_row,
}


def _build_properties(d: DocReader):
    for key, val in d.fields.items():
        if val is None:
            continue
        converter = _STAC_PROPERTY_MAP.get(key)
        if converter:
            yield from converter(key, val)


def uri_resolve(base: str, path: Optional[str]) -> str:
    """
    Backport of datacube.utils.uris.uri_resolve(), which isn't
    available on the stable release of datacube.
    """
    if path:
        p = Path(path)
        if p.is_absolute():
            return p.as_uri()

    return urljoin(base, path)
