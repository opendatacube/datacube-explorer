import json
import logging
from collections import defaultdict
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from functools import partial
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Sequence, Tuple, List
from urllib.parse import urljoin

import flask
from dateutil.tz import tz
from flask import abort, request
from werkzeug.datastructures import TypeConversionDict
from werkzeug.exceptions import HTTPException, BadRequest

from cubedash.summary._stores import DatasetItem
from datacube.model import Dataset, Range
from datacube.utils import DocReader, parse_time
from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc, ProductDoc, MeasurementDoc, AccessoryDoc
from eodatasets3.properties import StacPropertyView
from eodatasets3.scripts import tostac
from eodatasets3.utils import is_doc_eo3
from . import _model, _utils

_LOG = logging.getLogger(__name__)
bp = flask.Blueprint("stac", __name__, url_prefix="/stac")

PAGE_SIZE_LIMIT = _model.app.config.get("STAC_PAGE_SIZE_LIMIT", 1000)
DEFAULT_PAGE_SIZE = _model.app.config.get("STAC_DEFAULT_PAGE_SIZE", 20)
# Should we force all URLs to include the full hostname?
FORCE_ABSOLUTE_LINKS = _model.app.config.get("STAC_ABSOLUTE_HREFS", True)

_STAC_VERSION = "1.0.0-beta.2"
_STAC_DEFAULTS = dict(stac_version=_STAC_VERSION)


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
    description = config.get(
        "STAC_ENDPOINT_DESCRIPTION",
        "Configure stac endpoint information in your Explorer `settings.env.py` file",
    )
    if description:
        o["description"] = description
    return o


def utc(d: datetime):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


@bp.route("")
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
                            ".collection", collection=product.name, _external=True
                        ),
                    )
                    for product, product_summary in _model.get_products_with_summaries()
                ),
                dict(rel="self", href=request.url),
            ],
        )
    )


@bp.route("/search", methods=["GET", "POST"])
def stac_search():
    """
    Search api for stac items.
    """
    if request.method == "GET":
        args = request.args
    else:
        args = TypeConversionDict(request.get_json())

    products = args.get("collections", default=[], type=_array_arg)
    if "collection" in args:
        products.append(args.get("collection"))
    # Fallback for legacy 'product' argument
    elif "product" in args:
        products.append(args.get("product"))

    return _utils.as_geojson(_handle_search_request(args, products))


def _array_arg(arg: str, expect_size=None) -> List:
    """
    Parse an argument that should be a simple list.
    """
    if isinstance(arg, list):
        return arg

    # Make invalid arguments loud. The default ValueError behaviour is to quietly forget the param.
    try:
        value = json.loads(arg)
    except JSONDecodeError:
        raise BadRequest(
            f"Invalid argument syntax. Expected json-like list, got: {arg!r}"
        )

    if not isinstance(value, list):
        raise BadRequest(f"Invalid argument syntax. Expected json list, got: {value!r}")

    if expect_size is not None and len(value) != expect_size:
        raise BadRequest(
            f"Expected size {expect_size}, got {len(value)} elements in {arg!r}"
        )

    return value


def _handle_search_request(
    request_args: TypeConversionDict,
    product_names: List[str],
    route_name=".stac_search",
) -> Dict:
    bbox = request_args.get("bbox", type=partial(_array_arg, expect_size=4))
    time = request_args.get("time")
    limit = request_args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
    ids = request_args.get("ids", default=None, type=_array_arg)
    offset = request_args.get("_o", default=0, type=int)

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
            route_name,
            collections=product_names,
            bbox="[{},{},{},{}]".format(*bbox) if bbox else None,
            time=_unparse_time_range(time) if time else None,
            ids=json.dumps(ids) if ids else None,
            limit=limit,
            _o=next_offset,
        )

    return search_stac_items(
        product_names=product_names,
        bbox=bbox,
        time=time,
        dataset_ids=ids,
        limit=limit,
        offset=offset,
        get_next_url=next_page_url,
    )


def search_stac_items(
    get_next_url: Callable[[int], str],
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
    dataset_ids: Optional[str] = None,
    product_names: Optional[List[str]] = None,
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
            product_names=product_names,
            time=time,
            bbox=bbox,
            limit=limit + 1,
            dataset_ids=dataset_ids,
            offset=offset,
            full_dataset=True,
        )
    )
    returned = items[:limit]
    there_are_more = len(items) == limit + 1

    result = dict(
        **_STAC_DEFAULTS,
        stac_extensions=["context"],
        type="FeatureCollection",
        features=[as_stac_item(f) for f in returned],
        context=dict(page=offset // limit, limit=limit, returned=len(returned)),
        links=[],
    )

    if there_are_more:
        result["links"].append(dict(rel="next", href=get_next_url(offset + limit)))

    return result


@bp.route("/collections")
def list_collections():
    """
    This is like the root "/", but has full information for each collection in
     an array (instead of just a link to each collection).
    """
    return _utils.as_json(
        dict(
            **_STAC_DEFAULTS,
            links=[
                # TODO: Link to... root, I guess?
            ],
            collections=[
                _stac_collection(product.name)
                for product, product_summary in _model.get_products_with_summaries()
            ],
        )
    )


@bp.route("/collections/<collection>")
def collection(collection: str):
    """
    Overview of a WFS Collection (a datacube product)
    """
    return _utils.as_geojson(_stac_collection(collection))


def _stac_collection(collection: str):
    summary = _model.get_product_summary(collection)
    dataset_type = _model.STORE.get_dataset_type(collection)
    all_time_summary = _model.get_time_summary(collection)

    summary_props = {}
    if summary and summary.time_earliest:
        begin, end = utc(summary.time_earliest), utc(summary.time_latest)
        extent = {"temporal": {"interval": [[begin, end]]}}
        footprint = all_time_summary.footprint_wgs84
        if footprint:
            extent["spatial"] = {"bbox": [footprint.bounds]}

        summary_props["extent"] = extent
    stac_collection = dict(
        **_STAC_DEFAULTS,
        id=summary.name,
        title=summary.name,
        license=_utils.product_license(dataset_type),
        description=dataset_type.definition.get("description"),
        properties=dict(_build_properties(dataset_type.metadata)),
        providers=[],
        **summary_props,
        links=[
            dict(
                rel="items",
                href=url_for(
                    ".collection_items", collection=collection, _external=True
                ),
            )
        ],
    )
    return stac_collection


@bp.route("/collections/<collection>/items")
def collection_items(collection: str):
    """
    A geojson FeatureCollection of all items in a collection/product.

    (with paging)
    """
    all_time_summary = _model.get_time_summary(collection)
    if not all_time_summary:
        abort(404, "Product not yet summarised")

    feature_collection = _handle_search_request(
        request_args=request.args,
        product_names=[collection],
        route_name=".collection_items",
    )
    feature_collection["context"]["matched"] = all_time_summary.dataset_count
    return _utils.as_geojson(feature_collection)


@bp.route("/collections/<collection>/items/<dataset_id>")
def item(collection, dataset_id):
    dataset = _model.STORE.get_item(dataset_id)
    if not dataset:
        abort(404, "No such dataset")

    actual_product_name = dataset.product_name
    if collection != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're unkind)
        actual_url = url_for(
            ".item",
            collection=actual_product_name,
            dataset_id=dataset_id,
            _external=True,
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


def _band_to_measurement(band: Dict) -> MeasurementDoc:
    """Create EO3 measurement from an EO1 band dict"""
    return MeasurementDoc(
        path=band.get("path"),
        band=band.get("band"),
        layer=band.get("layer"),
        name=band.get("name"),
        alias=band.get("label"),
    )


def as_stac_item(dataset: DatasetItem):
    """
    Get a dict corresponding to a stac item
    """
    ds = dataset.odc_dataset

    if is_doc_eo3(ds.metadata_doc):
        dataset_doc = serialise.from_doc(ds.metadata_doc, skip_validation=True)
        dataset_doc.locations = ds.uris

        # Geometry is optional in eo3, and needs to be calculated from grids if missing.
        # We can use ODC's own calculation that happens on index.
        if dataset_doc.geometry is None:
            fallback_extent = ds.extent
            if fallback_extent is not None:
                dataset_doc.geometry = fallback_extent.geom
                dataset_doc.crs = str(ds.crs)

        if ds.sources:
            dataset_doc.lineage = {classifier: [d.id] for classifier, d in ds.sources}
        # Does ODC still put legacy lineage into indexed documents?
        elif ("source_datasets" in dataset_doc.lineage) and len(
            dataset_doc.lineage
        ) == 1:
            # From old to new lineage type.
            dataset_doc.lineage = {
                classifier: [dataset["id"]]
                for classifier, dataset in dataset_doc.lineage["source_datasets"]
            }

    else:
        # eo1 to eo3
        dataset_doc = DatasetDoc(
            id=ds.id,
            # Filled-in below.
            label=None,
            product=ProductDoc(dataset.product_name),
            locations=ds.uris,
            crs=dataset.geometry.crs.crs_str,
            geometry=dataset.geometry.geom,
            grids=None,
            # TODO: Convert these from stac to eo3
            properties=StacPropertyView(
                {
                    "datetime": utc(dataset.center_time),
                    **dict(_build_properties(dataset.odc_dataset.metadata)),
                    "odc:processing_datetime": utc(dataset.creation_time),
                }
            ),
            measurements={
                name: _band_to_measurement(b) for name, b in ds.measurements.items()
            },
            accessories=_accessories_from_eo1(ds.metadata_doc),
            # TODO: Fill in lineage. The datacube API only gives us full datasets, which is
            #       expensive. We only need a list of IDs here.
            lineage={},
        )

    if dataset_doc.label is None:
        dataset_doc.label = _utils.dataset_label(ds)

    item_doc = tostac.dataset_as_stac_item(
        dataset=dataset_doc,
        input_metadata_url=url_for("dataset.raw_doc", id_=ds.id),
        output_url=url_for(
            ".item",
            collection=dataset.product_name,
            dataset_id=dataset.dataset_id,
        ),
        explorer_base_url=url_for("default_redirect"),
    )
    # Add the region code that Explorer inferred.
    # (Explorer's region codes predate ODC's and support
    #  many more products.
    item_doc["properties"]["cubedash:region_code"] = dataset.region_code

    return item_doc


def _accessories_from_eo1(metadata_doc: Dict) -> Dict[str, AccessoryDoc]:
    """Create and EO3 accessories section from an EO1 document"""
    accessories = {}

    # Browse image -> thumbnail
    if "browse" in metadata_doc:
        for name, browse in metadata_doc["browse"].items():
            accessories[f"thumbnail:{name}"] = AccessoryDoc(
                path=browse["path"], name=name
            )

    # Checksum
    if "checksum_path" in metadata_doc:
        accessories["checksum:sha1"] = AccessoryDoc(
            path=metadata_doc["checksum_path"], name="checksum:sha1"
        )
    return accessories


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


def field_path_row(key, value):
    # Path/Row fields are ranges in datacube but 99% of the time
    # they are a single value
    # (they are ranges in telemetry products)
    # Stac doesn't accept a range here, so we'll skip it in those products,
    # but we can handle the 99% case when lower==higher.
    if key == "sat_path":
        kind = "landsat:wrs_path"
    elif key == "sat_row":
        kind = "landsat:wrs_row"
    else:
        raise ValueError(f"Path/row kind {repr(key)}")

    # If there's only one value in the range, return it.
    if isinstance(value, Range):
        if value.end is None or value.begin == value.end:
            # Standard stac
            yield kind, int(value.begin)
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


@bp.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    response = e.get_response()
    response.data = json.dumps(
        {
            "code": e.code,
            "name": e.name,
            "description": e.description,
        }
    )
    response.content_type = "application/json"
    return response
