import json
import logging
import uuid
from datetime import datetime, time as dt_time, timedelta
from functools import partial
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import flask
from datacube.model import Dataset, Range
from datacube.utils import DocReader, parse_time
from dateutil.tz import tz
from eodatasets3 import serialise, stac as eo3stac
from eodatasets3.model import AccessoryDoc, DatasetDoc, MeasurementDoc, ProductDoc
from eodatasets3.properties import Eo3Dict
from eodatasets3.utils import is_doc_eo3
from flask import abort, request
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from werkzeug.datastructures import TypeConversionDict
from werkzeug.exceptions import BadRequest, HTTPException

from cubedash.summary._stores import DatasetItem

from . import _model, _utils
from .summary import ItemSort

_LOG = logging.getLogger(__name__)
bp = flask.Blueprint("stac", __name__, url_prefix="/stac")

PAGE_SIZE_LIMIT = _model.app.config.get("STAC_PAGE_SIZE_LIMIT", 1000)
DEFAULT_PAGE_SIZE = _model.app.config.get("STAC_DEFAULT_PAGE_SIZE", 20)
# Should we force all URLs to include the full hostname?
FORCE_ABSOLUTE_LINKS = _model.app.config.get("STAC_ABSOLUTE_HREFS", True)

# Should searches return the full properties for every stac item by default?
# These searches are much slower we're forced us to use ODC's own metadata table.
DEFAULT_RETURN_FULL_ITEMS = _model.app.config.get(
    "STAC_DEFAULT_FULL_ITEM_INFORMATION", True
)

STAC_VERSION = "1.0.0"


def url_for(*args, **kwargs):
    if FORCE_ABSOLUTE_LINKS:
        kwargs["_external"] = True
    return flask.url_for(*args, **kwargs)


def stac_endpoint_information() -> Dict:
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


def _stac_response(doc: Dict, content_type="application/json") -> flask.Response:
    """Return a stac document as the flask response"""
    return _utils.as_json(
        _with_stac_properties(doc),
        content_type=content_type,
    )


def _with_stac_properties(doc):
    # Any response without a links array already is a coding problem.
    doc["links"].append(dict(rel="root", href=url_for(".root")))
    return {
        # Always put stac version at the beginning for readability.
        "stac_version": STAC_VERSION,
        # The given doc may override it too.
        **doc,
    }


def _geojson_stac_response(doc: Dict) -> flask.Response:
    """Return a stac item"""
    return _stac_response(doc, content_type="application/geo+json")


@bp.route("", strict_slashes=False)
def root():
    """
    The root stac page links to each collection (product) catalog
    """
    return _stac_response(
        dict(
            **stac_endpoint_information(),
            type="Catalog",
            links=[
                dict(
                    title="Collections",
                    description="All product collections",
                    rel="children",
                    type="application/json",
                    href=url_for(".collections"),
                ),
                dict(
                    title="Arrivals",
                    description="Most recently added items",
                    rel="child",
                    type="application/json",
                    href=url_for(".arrivals"),
                ),
                dict(
                    title="Item Search",
                    rel="search",
                    type="application/json",
                    href=url_for(".stac_search"),
                ),
                dict(rel="self", href=request.url),
                # Individual Product Collections
                *(
                    dict(
                        title=product.name,
                        description=product.definition.get("description"),
                        rel="child",
                        href=url_for(".collection", collection=product.name),
                    )
                    for product, product_summary in _model.get_products_with_summaries()
                ),
            ],
            conformsTo=[
                "https://api.stacspec.org/v1.0.0-beta.1/core",
                "https://api.stacspec.org/v1.0.0-beta.1/item-search",
                # Incomplete:
                # "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
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

    return _geojson_stac_response(_handle_search_request(args, products))


def _array_arg(arg: str, expect_type=str, expect_size=None) -> List:
    """
    Parse an argument that should be a simple list.
    """
    if isinstance(arg, list):
        return arg

    # Make invalid arguments loud. The default ValueError behaviour is to quietly forget the param.
    try:
        arg = arg.strip()
        # Legacy json-like format. This is what sat-api seems to do too.
        if arg.startswith("["):
            value = json.loads(arg)
        else:
            # Otherwise OpenAPI non-exploded form style.
            # Eg. "1, 2, 3" or "string1,string2" or "string1"
            args = [a.strip() for a in arg.split(",")]
            value = [expect_type(a.strip()) for a in args if a]
    except ValueError:
        raise BadRequest(
            f"Invalid argument syntax. Expected comma-separated list, got: {arg!r}"
        )

    if not isinstance(value, list):
        raise BadRequest(f"Invalid argument syntax. Expected json list, got: {value!r}")

    if expect_size is not None and len(value) != expect_size:
        raise BadRequest(
            f"Expected size {expect_size}, got {len(value)} elements in {arg!r}"
        )

    return value


def _geojson_arg(arg: dict) -> BaseGeometry:
    if not isinstance(arg, dict):
        raise BadRequest(
            "The 'intersects' argument must be a JSON object (and sent over a POST request)"
        )

    try:
        return shape(arg)
    except ValueError:
        raise BadRequest("The 'intersects' argument must be valid GeoJSON geometry.")


def _bool_argument(s: str):
    """
    Parse an argument that should be a bool
    """
    if isinstance(s, bool):
        return s
    # Copying FastAPI booleans:
    # https://fastapi.tiangolo.com/tutorial/query-params
    return s.strip().lower() in ("1", "true", "on", "yes")


def _handle_search_request(
    request_args: TypeConversionDict,
    product_names: List[str],
    require_geometry: bool = True,
    include_total_count: bool = True,
) -> Dict:
    bbox = request_args.get(
        "bbox", type=partial(_array_arg, expect_size=4, expect_type=float)
    )

    # Stac-api <=0.7.0 used 'time', later versions use 'datetime'
    time = request_args.get("datetime") or request_args.get("time")

    limit = request_args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
    ids = request_args.get(
        "ids", default=None, type=partial(_array_arg, expect_type=uuid.UUID)
    )
    offset = request_args.get("_o", default=0, type=int)

    # Request the full Item information. This forces us to go to the
    # ODC dataset table for every record, which can be extremely slow.
    full_information = request_args.get(
        "_full", default=DEFAULT_RETURN_FULL_ITEMS, type=_bool_argument
    )

    intersects = request_args.get("intersects", default=None, type=_geojson_arg)

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
            collections=product_names,
            bbox="{},{},{},{}".format(*bbox) if bbox else None,
            time=_unparse_time_range(time) if time else None,
            ids=",".join(map(str, ids)) if ids else None,
            limit=limit,
            _o=next_offset,
            _full=full_information,
        )

    feature_collection = search_stac_items(
        product_names=product_names,
        bbox=bbox,
        time=time,
        dataset_ids=ids,
        limit=limit,
        offset=offset,
        intersects=intersects,
        # The /stac/search api only supports intersects over post requests.
        use_post_request=intersects is not None,
        get_next_url=next_page_url,
        full_information=full_information,
        require_geometry=require_geometry,
        include_total_count=include_total_count,
    )
    feature_collection["links"].extend(
        (
            dict(
                href=url_for(".stac_search"),
                rel="search",
                title="Search",
                type="application/geo+json",
                method="GET",
            ),
            dict(
                href=url_for(".stac_search"),
                rel="search",
                title="Search",
                type="application/geo+json",
                method="POST",
            ),
        )
    )
    return feature_collection


def search_stac_items(
    get_next_url: Callable[[int], str],
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
    dataset_ids: Optional[str] = None,
    product_names: Optional[List[str]] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    intersects: Optional[BaseGeometry] = None,
    time: Optional[Tuple[datetime, datetime]] = None,
    full_information: bool = False,
    order: ItemSort = ItemSort.DEFAULT_SORT,
    require_geometry: bool = True,
    include_total_count: bool = False,
    use_post_request: bool = False,
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
            intersects=intersects,
            offset=offset,
            full_dataset=full_information,
            order=order,
            require_geometry=require_geometry,
        )
    )
    returned = items[:limit]
    there_are_more = len(items) == limit + 1

    page = 0
    if limit != 0:
        page = offset // limit
    paging_properties = dict(
        # Stac standard
        numberReturned=len(returned),
        # Compatibility with older implementation. Was removed from stac-api standard.
        # (page numbers + limits are not ideal as they prevent some big db optimisations.)
        context=dict(
            page=page,
            limit=limit,
            returned=len(returned),
        ),
    )
    if include_total_count:
        count_matching = _model.STORE.get_count(
            product_names=product_names, time=time, bbox=bbox, dataset_ids=dataset_ids
        )
        paging_properties["numberMatched"] = count_matching
        paging_properties["context"]["matched"] = count_matching

    result = dict(
        type="FeatureCollection",
        features=[as_stac_item(f) for f in returned],
        links=[],
        **paging_properties,
    )

    if there_are_more:
        if use_post_request:
            next_link = dict(
                rel="next",
                method="POST",
                merge=True,
                # Unlike GET requests, we can tell them to repeat their same request args
                # themselves.
                #
                # Same URL:
                href=flask.request.url,
                # ... with a new offset.
                body=dict(
                    _o=offset + limit,
                ),
            )
        else:
            # Otherwise, let the route create the next url.
            next_link = dict(rel="next", href=get_next_url(offset + limit))

        result["links"].append(next_link)

    return result


@bp.route("/collections")
def collections():
    """
    This is like the root "/", but has full information for each collection in
     an array (instead of just a link to each collection).
    """
    return _stac_response(
        dict(
            links=[],
            collections=[
                _with_stac_properties(_stac_collection(product.name))
                for product, product_summary in _model.get_products_with_summaries()
            ],
        )
    )


@bp.route("/arrivals")
def arrivals():
    """
    Virtual collection of the items most recently indexed into this index
    """
    return _stac_response(
        dict(
            id="Arrivals",
            title="Dataset Arrivals",
            type="Collection",
            license="various",
            description="The most recently added Items to this index",
            properties={},
            providers=[],
            # Covers all products, so all possible extent. We *could* be smart and show the whole
            # server's extent range, but that wouldn't be too useful either. ?
            extent=dict(
                temporal=dict(interval=[[None, None]]),
                spatial=dict(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            ),
            links=[
                dict(
                    rel="items",
                    href=url_for(".arrivals_items"),
                )
            ],
        )
    )


@bp.route("/arrivals/items")
def arrivals_items():
    """
    Get the Items most recently indexed into this Open Data Cube instance.

    This returns a Stac FeatureCollection of complete Stac Items, with paging links.
    """
    limit = request.args.get("limit", default=DEFAULT_PAGE_SIZE, type=int)
    offset = request.args.get("_o", default=0, type=int)
    if limit > PAGE_SIZE_LIMIT:
        abort(
            400,
            f"Max page size is {PAGE_SIZE_LIMIT}. "
            f"Use the next links instead of a large limit.",
        )

    def next_page_url(next_offset):
        return url_for(
            ".arrivals_items",
            limit=limit,
            _o=next_offset,
        )

    return _geojson_stac_response(
        search_stac_items(
            limit=limit,
            offset=offset,
            get_next_url=next_page_url,
            full_information=True,
            order=ItemSort.RECENTLY_ADDED,
            require_geometry=False,
            include_total_count=False,
        )
    )


@bp.route("/collections/<collection>")
def collection(collection: str):
    """
    Overview of a WFS Collection (a datacube product)
    """
    return _stac_response(_stac_collection(collection))


def _stac_collection(collection: str):
    summary = _model.get_product_summary(collection)
    try:
        dataset_type = _model.STORE.get_dataset_type(collection)
    except KeyError:
        abort(404, f"Unknown collection {collection!r}")

    all_time_summary = _model.get_time_summary(collection)

    begin, end = (
        (summary.time_earliest, summary.time_latest) if summary else (None, None)
    )
    footprint = all_time_summary.footprint_wgs84
    stac_collection = dict(
        id=summary.name,
        title=summary.name,
        type="Collection",
        license=_utils.product_license(dataset_type),
        description=dataset_type.definition.get("description"),
        properties=dict(_build_properties(dataset_type.metadata)),
        providers=[],
        extent=dict(
            temporal=dict(
                interval=[
                    [
                        utc(begin) if begin else None,
                        utc(end) if end else None,
                    ]
                ]
            ),
            spatial=dict(
                bbox=[footprint.bounds if footprint else [-180.0, -90.0, 180.0, 90.0]]
            ),
        ),
        links=[
            dict(
                rel="items",
                href=url_for(".collection_items", collection=collection),
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
        abort(404, f"Product {collection!r} not found among summaries.")

    feature_collection = _handle_search_request(
        request_args=request.args,
        product_names=[collection],
    )

    # Maybe we shouldn't include total count, as it prevents some future optimisation?
    if "numberMatched" not in feature_collection:
        feature_collection["numberMatched"] = all_time_summary.dataset_count
    # Backwards compatibility with older stac implementations.
    feature_collection["context"]["matched"] = feature_collection["numberMatched"]

    return _geojson_stac_response(feature_collection)


@bp.route("/collections/<collection>/items/<dataset_id>")
def item(collection: str, dataset_id: str):
    dataset = _model.STORE.get_item(dataset_id)
    if not dataset:
        abort(404, f"No dataset found with id {dataset_id!r}")

    actual_product_name = dataset.product_name
    if collection != actual_product_name:
        # We're not doing a redirect as we don't want people to rely on wrong urls
        # (and we're unkind)
        actual_url = url_for(
            ".item",
            collection=actual_product_name,
            dataset_id=dataset_id,
        )
        abort(
            404,
            f"No such dataset in collection.\n"
            f"Perhaps you meant collection {actual_product_name}: {actual_url})",
        )

    return _geojson_stac_response(as_stac_item(dataset))


def _pick_remote_uri(uris: Sequence[str]) -> Optional[int]:
    """
    Return the offset of the first uri with a remote path, if any.
    """
    for i, uri in enumerate(uris):
        scheme, *_ = uri.split(":")
        if scheme in ("https", "http", "ftp", "s3", "gfs"):
            return i
    return None


def _parse_time_range(time: str) -> Optional[Tuple[datetime, datetime]]:
    """
    >>> _parse_time_range('1986-04-16T01:12:16/2097-05-10T00:24:21')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(2097, 5, 10, 0, 24, 21))
    >>> _parse_time_range('1986-04-16T01:12:16')
    (datetime.datetime(1986, 4, 16, 1, 12, 16), datetime.datetime(1986, 4, 16, 1, 12, 17))
    >>> # Time is optional:
    >>> _parse_time_range('2019-01-01/2019-01-01')
    (datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 1, 0, 0))
    >>> _parse_time_range('1986-04-16')
    (datetime.datetime(1986, 4, 16, 0, 0), datetime.datetime(1986, 4, 17, 0, 0))
    >>> # Open ranges:
    >>> _parse_time_range('2019-01-01/..')[0]
    datetime.datetime(2019, 1, 1, 0, 0)
    >>> _parse_time_range('2019-01-01/..')[1] > datetime.now()
    True
    >>> _parse_time_range('../2019-01-01')
    (datetime.datetime(1971, 1, 1, 0, 0), datetime.datetime(2019, 1, 1, 0, 0))
    >>> # Unbounded time is the same as no time filter. ("None")
    >>> _parse_time_range('../..')
    >>>
    """
    time_period = time.split("/")
    if len(time_period) == 2:
        start, end = time_period
        if start == "..":
            start = datetime(1971, 1, 1, 0, 0)
        elif end == "..":
            end = datetime.now() + timedelta(days=2)
        # Were they both open? Treat it as no date filter.
        if end == "..":
            return None

        return parse_time(start), parse_time(end)
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


def _band_to_measurement(band: Dict, dataset_location: str) -> MeasurementDoc:
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
    ds: Dataset = dataset.odc_dataset

    if ds is not None and is_doc_eo3(ds.metadata_doc):
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
            id=dataset.dataset_id,
            # Filled-in below.
            label=None,
            product=ProductDoc(dataset.product_name),
            locations=ds.uris if ds is not None else None,
            crs=str(dataset.geometry.crs),
            geometry=dataset.geometry.geom,
            grids=None,
            # TODO: Convert these from stac to eo3
            properties=Eo3Dict(
                {
                    "datetime": utc(dataset.center_time),
                    **(dict(_build_properties(ds.metadata)) if ds else {}),
                    "odc:processing_datetime": utc(dataset.creation_time),
                }
            ),
            measurements={
                name: _band_to_measurement(
                    b,
                    dataset_location=ds.uris[0] if ds is not None and ds.uris else None,
                )
                for name, b in ds.measurements.items()
            }
            if ds is not None
            else {},
            accessories=_accessories_from_eo1(ds.metadata_doc)
            if ds is not None
            else {},
            # TODO: Fill in lineage. The datacube API only gives us full datasets, which is
            #       expensive. We only need a list of IDs here.
            lineage={},
        )

    if dataset_doc.label is None and ds is not None:
        dataset_doc.label = _utils.dataset_label(ds)

    item_doc = eo3stac.to_stac_item(
        dataset=dataset_doc,
        stac_item_destination_url=url_for(
            ".item",
            collection=dataset.product_name,
            dataset_id=dataset.dataset_id,
        ),
        odc_dataset_metadata_url=url_for("dataset.raw_doc", id_=dataset.dataset_id),
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
            yield f"odc:{key}", [value.begin, value.end]


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
