import json
import logging
import uuid
from datetime import datetime, timedelta
from datetime import time as dt_time
from functools import partial
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

import flask
import pystac
from datacube.model import Dataset, Range
from datacube.utils import DocReader, parse_time
from dateutil.tz import tz
from eodatasets3 import serialise
from eodatasets3 import stac as eo3stac
from eodatasets3.model import AccessoryDoc, DatasetDoc, MeasurementDoc, ProductDoc
from eodatasets3.properties import Eo3Dict
from eodatasets3.utils import is_doc_eo3
from flask import abort, request
from pystac import Catalog, Collection, Extent, ItemCollection, Link, STACObject
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from toolz import dicttoolz
from werkzeug.datastructures import TypeConversionDict
from werkzeug.exceptions import BadRequest, HTTPException

from cubedash.summary._stores import DatasetItem

from . import _model, _utils
from .summary import ItemSort

_LOG = logging.getLogger(__name__)
bp = flask.Blueprint("stac", __name__, url_prefix="/stac")

PAGE_SIZE_LIMIT = _model.app.config.get("STAC_PAGE_SIZE_LIMIT", 1000)
DEFAULT_PAGE_SIZE = _model.app.config.get("STAC_DEFAULT_PAGE_SIZE", 20)
DEFAULT_CATALOG_SIZE = _model.app.config.get("STAC_DEFAULT_CATALOG_SIZE", 500)

# Should we force all URLs to include the full hostname?
FORCE_ABSOLUTE_LINKS = _model.app.config.get("STAC_ABSOLUTE_HREFS", True)

# Should searches return the full properties for every stac item by default?
# These searches are much slower we're forced us to use ODC's own metadata table.
DEFAULT_RETURN_FULL_ITEMS = _model.app.config.get(
    "STAC_DEFAULT_FULL_ITEM_INFORMATION", True
)

STAC_VERSION = "1.0.0"

############################
#  Helpers
############################

# Time-related


def utc(d: datetime):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    return d.astimezone(tz.tzutc())


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


# URL-related


def url_for(*args, **kwargs):
    if FORCE_ABSOLUTE_LINKS:
        kwargs["_external"] = True
    return flask.url_for(*args, **kwargs)


def _pick_remote_uri(uris: Sequence[str]) -> Optional[int]:
    """
    Return the offset of the first uri with a remote path, if any.
    """
    for i, uri in enumerate(uris):
        scheme, *_ = uri.split(":")
        if scheme in ("https", "http", "ftp", "s3", "gfs"):
            return i
    return None


# Conversions


def _band_to_measurement(band: Dict, dataset_location: str) -> MeasurementDoc:
    """Create EO3 measurement from an EO1 band dict"""
    return MeasurementDoc(
        path=band.get("path"),
        band=band.get("band"),
        layer=band.get("layer"),
        name=band.get("name"),
        alias=band.get("label"),
    )


def as_stac_item(dataset: DatasetItem) -> pystac.Item:
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
            crs=str(dataset.geometry.crs) if dataset.geometry is not None else None,
            geometry=dataset.geometry.geom if dataset.geometry is not None else None,
            grids=None,
            # TODO: Convert these from stac to eo3
            properties=Eo3Dict(
                {
                    "datetime": utc(dataset.center_time),
                    **(dict(_build_properties(ds.metadata)) if ds else {}),
                    "odc:processing_datetime": utc(dataset.creation_time),
                }
            ),
            measurements=(
                {
                    name: _band_to_measurement(
                        b,
                        dataset_location=(
                            ds.uris[0] if ds is not None and ds.uris else None
                        ),
                    )
                    for name, b in ds.measurements.items()
                }
                if ds is not None
                else {}
            ),
            accessories=(
                _accessories_from_eo1(ds.metadata_doc) if ds is not None else {}
            ),
            # TODO: Fill in lineage. The datacube API only gives us full datasets, which is
            #       expensive. We only need a list of IDs here.
            lineage={},
        )

    if dataset_doc.label is None and ds is not None:
        dataset_doc.label = _utils.dataset_label(ds)

    item = eo3stac.to_pystac_item(
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
    item.properties["cubedash:region_code"] = dataset.region_code

    # add canonical ref pointing to the JSON file on s3
    if ds is not None and ds.uris:
        media_type = "application/json" if ds.uris[0].endswith("json") else "text/yaml"
        item.links.append(
            Link(
                rel="canonical",
                media_type=media_type,
                target=_utils.as_resolved_remote_url(None, ds.uris[0]),
            )
        )

    return item


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
        raise ValueError(f"Path/row kind {key!r}")

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


# Search arguments


def _array_arg(
    arg: Union[str, List[Union[str, float]]], expect_type=str, expect_size=None
) -> List:
    """
    Parse an argument that should be a simple list.
    """
    if isinstance(arg, list):
        return arg

    # Make invalid arguments loud. The default ValueError behaviour is to quietly forget the param.
    try:
        if not isinstance(arg, str):
            raise ValueError
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


def _dict_arg(arg: dict):
    """
    Parse stac extension arguments as dicts
    """
    if isinstance(arg, str):
        arg = json.loads(arg.replace("'", '"'))
    return arg


def _list_arg(arg: list):
    """
    Parse sortby argument as a list of dicts
    """
    if isinstance(arg, str):
        arg = list(arg)
    return list(
        map(lambda a: json.loads(a.replace("'", '"')) if isinstance(a, str) else a, arg)
    )


# Search


def _handle_search_request(
    method: str,
    request_args: TypeConversionDict,
    product_names: List[str],
    include_total_count: bool = True,
) -> ItemCollection:
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

    query = request_args.get("query", default=None, type=_dict_arg)

    fields = request_args.get("fields", default=None, type=_dict_arg)

    sortby = request_args.get("sortby", default=None, type=_list_arg)

    filter_cql = request_args.get("filter", default=None, type=_dict_arg)

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
            collections=",".join(product_names),
            bbox="{},{},{},{}".format(*bbox) if bbox else None,
            time=_unparse_time_range(time) if time else None,
            ids=",".join(map(str, ids)) if ids else None,
            limit=limit,
            _o=next_offset,
            _full=full_information,
            query=query,
            fields=fields,
            sortby=sortby,
            filter=filter_cql,
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
        use_post_request=method == "POST" or intersects is not None,
        get_next_url=next_page_url,
        full_information=full_information,
        include_total_count=include_total_count,
        query=query,
        fields=fields,
        sortby=sortby,
        filter_cql=filter_cql,
    )

    feature_collection.extra_fields["links"].extend(
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


# Item search extensions


def _get_property(prop: str, item: pystac.Item, no_default=False):
    """So that we don't have to keep using this bulky expression"""
    return dicttoolz.get_in(prop.split("."), item.to_dict(), no_default=no_default)


def _predicate_helper(items: List[pystac.Item], prop: str, op: str, val) -> filter:
    """Common comparison predicates used in both query and filter"""
    if op == "eq" or op == "=":
        return filter(lambda item: _get_property(prop, item) == val, items)
    if op == "gte" or op == ">=":
        return filter(lambda item: _get_property(prop, item) >= val, items)
    if op == "lte" or op == "<=":
        return filter(lambda item: _get_property(prop, item) <= val, items)
    elif op == "gt" or op == ">":
        return filter(lambda item: _get_property(prop, item) > val, items)
    elif op == "lt" or op == "<":
        return filter(lambda item: _get_property(prop, item) < val, items)
    elif op == "neq" or op == "<>":
        return filter(lambda item: _get_property(prop, item) != val, items)


def _handle_query_extension(items: List[pystac.Item], query: dict) -> List[pystac.Item]:
    """
    Implementation of item search query extension (https://github.com/stac-api-extensions/query/blob/main/README.md)
    The documentation doesn't specify whether multiple properties should be treated as logical AND or OR; this
    implementation has assumed AND.

    query = {'property': {'op': 'value'}, 'property': {'op': 'value', 'op': 'value'}}
    """
    filtered = items
    # split on '.' to use dicttoolz for nested items
    for prop in query.keys():
        # Retrieve nested dict values
        for op, val in query[prop].items():
            if op == "startsWith":
                matched = filter(
                    lambda item: _get_property(prop, item).startswith(val), items
                )
            elif op == "endsWith":
                matched = filter(
                    lambda item: _get_property(prop, item).endswith(val), items
                )
            elif op == "contains":
                matched = filter(lambda item: val in _get_property(prop, item), items)
            elif op == "in":
                matched = filter(lambda item: _get_property(prop, item) in val, items)
            else:
                matched = _predicate_helper(items, prop, op, val)

            # achieve logical and between queries with set intersection
            filtered = list(set(filtered).intersection(set(matched)))

    return filtered


def _handle_fields_extension(
    items: List[pystac.Item], fields: dict
) -> List[pystac.Item]:
    """
    Implementation of fields extension (https://github.com/stac-api-extensions/fields/blob/main/README.md)
    This implementation differs slightly from the documented semantics in that if only `exclude` is specified, those
    attributes will be subtracted from the complete set of the item's attributes, not just the default. `exclude` will
    also not remove any of the default attributes so as to prevent errors due to invalid stac items.

    fields = {'include': [...], 'exclude': [...]}
    """
    res = []
    # minimum fields needed for a valid stac item
    default_fields = [
        "id",
        "type",
        "geometry",
        "bbox",
        "links",
        "assets",
        "properties.datetime",
        "stac_version",
    ]

    for item in items:
        include = fields.get("include") or []
        # if 'include' is provided we build up from an empty slate;
        # but if only 'exclude' is provided we remove from all existing fields
        filtered_item = {} if fields.get("include") else item.to_dict()
        # union of 'include' and default fields to ensure a valid stac item
        include = list(set(include + default_fields))

        for inc in include:
            filtered_item = dicttoolz.update_in(
                d=filtered_item,
                keys=inc.split("."),
                # get corresponding field from item
                # disallow default to avoid None values being inserted
                func=lambda _: _get_property(inc, item, no_default=True),
            )

        for exc in fields.get("exclude") or []:
            # don't remove a field if it will make for an invalid stac item
            if exc not in default_fields:
                # what about a field that isn't there?
                split = exc.split(".")
                # have to manually take care of nested case because dicttoolz doesn't have a dissoc_in
                if len(split):
                    filtered_item[split[0]] = dicttoolz.dissoc(
                        filtered_item[split[0]], split[1]
                    )
                else:
                    filtered_item = dicttoolz.dissoc(filtered_item, exc)

        res.append(pystac.Item.from_dict(filtered_item))

    return res


def _handle_sortby_extension(
    items: List[pystac.Item], sortby: List[dict]
) -> List[pystac.Item]:
    """
    Implementation of sort extension (https://github.com/stac-api-extensions/sort/blob/main/README.md)

    sortby = [ {'field': 'field_name', 'direction': <'asc' or 'desc'>} ]
    """
    sorted_items = items

    for s in sortby:
        field = s.get("field")
        reverse = s.get("direction") == "desc"
        # should we enforce correct names and raise error if not?
        sorted_items = sorted(
            sorted_items, key=lambda i: _get_property(field, i), reverse=reverse
        )

    return list(sorted_items)


def _handle_filter_extension(
    items: List[pystac.Item], filter_cql: dict
) -> List[pystac.Item]:
    """
    Implementation of filter extension (https://github.com/stac-api-extensions/filter/blob/main/README.md)
    Currently only supporting logical expression (and/or), null and binary comparisons, provided in cql-json
    Assumes comparisons to be done between a property value and a literal

    filter = {'op': 'and','args':
    [{'op': '=', 'args': [{'property': 'prop_name'}, val]}, {'op': 'isNull', 'args': {'property': 'prop_name'}}]
    }
    """
    results = []
    op = filter_cql.get("op")
    args = filter_cql.get("args")
    # if there is a nested operation in the args, recur to resolve those, creating
    # a list of lists that we can then apply the top level operator to
    for arg in [a for a in args if isinstance(a, dict) and a.get("op")]:
        results.append(_handle_filter_extension(items, arg))

    if op == "and":
        # set intersection between each result
        # need to pass results as a list of sets to intersection
        results = list(set.intersection(*map(set, results)))
    elif op == "or":
        # set union between each result
        results = list(set.union(*map(set, results)))
    elif op == "isNull":
        # args is a single property rather than a list
        prop = args.get("property")
        results = filter(
            lambda item: _get_property(prop, item) in [None, "None"], items
        )
    else:
        prop = args[0].get("property")
        val = args[1]
        results = _predicate_helper(items, prop, op, val)

    return list(results)


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
    include_total_count: bool = False,
    use_post_request: bool = False,
    query: Optional[dict] = None,
    fields: Optional[dict] = None,
    sortby: Optional[List[dict]] = None,
    filter_cql: Optional[dict] = None,
) -> ItemCollection:
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
        )
    )
    returned = items[:limit]
    there_are_more = len(items) == limit + 1

    page = 0
    if limit != 0:
        page = offset // limit
    extra_properties = dict(
        links=[],
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
        extra_properties["numberMatched"] = count_matching
        extra_properties["context"]["matched"] = count_matching

    items = [as_stac_item(f) for f in returned]
    items = _handle_query_extension(items, query) if query else items
    items = _handle_filter_extension(items, filter_cql) if filter_cql else items
    items = _handle_sortby_extension(items, sortby) if sortby else items
    items = _handle_fields_extension(items, fields) if fields else items

    result = ItemCollection(items, extra_fields=extra_properties)

    if there_are_more:
        next_link = dict(
            rel="next",
            title="Next page of Items",
            type="application/geo+json",
        )
        if use_post_request:
            next_link.update(
                dict(
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
            )
        else:
            # Otherwise, let the route create the next url.
            next_link.update(
                dict(
                    method="GET",
                    href=get_next_url(offset + limit),
                )
            )

        result.extra_fields["links"].append(next_link)

    return result


# Response helpers


def _stac_collection(collection: str) -> Collection:
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
    if "title" in dataset_type.definition.get("metadata"):
        title = dataset_type.definition.get("metadata")["title"]
    else:
        title = summary.name
    stac_collection = Collection(
        id=summary.name,
        title=title,
        license=_utils.product_license(dataset_type),
        description=dataset_type.definition.get("description"),
        providers=[],
        extent=Extent(
            pystac.SpatialExtent(
                bboxes=[footprint.bounds if footprint else [-180.0, -90.0, 180.0, 90.0]]
            ),
            temporal=pystac.TemporalExtent(
                intervals=[
                    [
                        utc(begin) if begin else None,
                        utc(end) if end else None,
                    ]
                ]
            ),
        ),
    )
    stac_collection.set_root(root_catalog())

    stac_collection.links.extend(
        [
            Link(rel="self", target=request.url),
            Link(
                rel="items",
                target=url_for(".collection_items", collection=collection),
            ),
            Link(
                rel="http://www.opengis.net/def/rel/ogc/1.0/queryables",
                target=url_for(".collection_queryables", collection=collection),
            ),
        ]
    )
    if all_time_summary.timeline_dataset_counts:
        stac_collection.links.extend(
            Link(
                rel="child",
                target=url_for(
                    ".collection_month",
                    collection=collection,
                    year=date.year,
                    month=date.month,
                ),
            )
            for date, count in all_time_summary.timeline_dataset_counts.items()
            if count > 0
        )
    return stac_collection


def _stac_response(
    doc: Union[STACObject, ItemCollection], content_type="application/json"
) -> flask.Response:
    """Return a stac document as the flask response"""
    if isinstance(doc, STACObject):
        doc.set_root(root_catalog())
    return _utils.as_json(
        doc.to_dict(),
        content_type=content_type,
    )


def _geojson_stac_response(doc: Union[STACObject, ItemCollection]) -> flask.Response:
    """Return a stac item"""
    return _stac_response(doc, content_type="application/geo+json")


# Root setup


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


def root_catalog():
    c = Catalog(
        **stac_endpoint_information(),
    )
    c.set_self_href(url_for(".root"))
    return c


##########################
# ENDPOINTS
##########################


@bp.route("", strict_slashes=False)
def root():
    """
    The root stac page links to each collection (product) catalog
    """
    c = root_catalog()
    c.links.extend(
        [
            Link(
                title="Collections",
                # description="All product collections",
                rel="children",
                media_type="application/json",
                target=url_for(".collections"),
            ),
            Link(
                title="Arrivals",
                # description="Most recently added items",
                rel="child",
                media_type="application/json",
                target=url_for(".arrivals"),
            ),
            Link(
                title="Item Search",
                rel="search",
                media_type="application/json",
                target=url_for(".stac_search"),
            ),
            Link(
                title="Queryables",
                rel="http://www.opengis.net/def/rel/ogc/1.0/queryables",
                media_type="application/json",
                target=url_for(".queryables"),
            ),
            # Individual Product Collections
            *(
                Link(
                    title=product.name,
                    # description=product.definition.get("description"),
                    rel="child",
                    media_type="application/json",
                    target=url_for(".collection", collection=product.name),
                )
                for product, product_summary in _model.get_products_with_summaries()
            ),
        ]
    )
    conformance_classes = [
        "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
        "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
        "https://api.stacspec.org/v1.0.0-rc.1/core",
        "https://api.stacspec.org/v1.0.0-rc.1/item-search",
        "https://api.stacspec.org/v1.0.0-rc.1/ogcapi-features",
        "https://api.stacspec.org/v1.0.0-rc.1/item-search#query",
        "https://api.stacspec.org/v1.0.0-rc.1/item-search#fields",
        "https://api.stacspec.org/v1.0.0-rc.1/ogcapi-features#fields",
        "https://api.stacspec.org/v1.0.0-rc.1/item-search#sort",
        "https://api.stacspec.org/v1.0.0-rc.1/ogcapi-features#sort",
        "https://api.stacspec.org/v1.0.0-rc.1/item-search#filter",
        "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
        "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
        "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
        "https://api.stacspec.org/v1.0.0-rc.1/collections",
    ]
    c.extra_fields = dict(conformsTo=conformance_classes)

    return _stac_response(c)


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

    return _geojson_stac_response(
        _handle_search_request(request.method, args, products)
    )


# Collections


@bp.route("/collections")
def collections():
    """
    This is like the root "/", but has full information for each collection in
     an array (instead of just a link to each collection).
    """
    return _utils.as_json(
        dict(
            links=[
                dict(rel="self", type="application/json", href=request.url),
                dict(rel="root", type="application/json", href=url_for(".root")),
                dict(rel="parent", type="application/json", href=url_for(".root")),
            ],
            collections=[
                # TODO: This has a root link, right?
                _stac_collection(product.name).to_dict()
                for product, product_summary in _model.get_products_with_summaries()
            ],
        )
    )


@bp.route("/queryables")
def queryables():
    """
    Define what terms are available for use when writing filter expressions for the entire catalog
    Part of basic CQL2 conformance for stac-api filter implementation.
    See: https://github.com/stac-api-extensions/filter#queryables
    """
    return _utils.as_json(
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": flask.request.base_url,
            "type": "object",
            "title": "",
            "properties": {
                "id": {
                    "title": "Item ID",
                    "description": "Item identifier",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "item.json#/definitions/core/allOf/2/properties/id",
                },
                "collection": {
                    "description": "Collection",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "item.json#/collection",
                },
                "geometry": {
                    "description": "Geometry",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/geometry",
                },
                "datetime": {
                    "description": "Datetime",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "datetime.json#/properties/datetime",
                },
            },
            "additionalProperties": True,
        }
    )


@bp.route("/collections/<collection>/queryables")
def collection_queryables(collection: str):
    """
    The queryables resources for a given collection (barebones implementation)
    """
    try:
        dataset_type = _model.STORE.get_dataset_type(collection)
    except KeyError:
        abort(404, f"Unknown collection {collection!r}")

    collection_title = dataset_type.definition.get("description")
    return _utils.as_json(
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": flask.request.base_url,
            "type": "object",
            "title": f"Queryables for {collection_title}",
            "properties": {
                "id": {
                    "title": "Item ID",
                    "description": "Item identifier",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "item.json#/definitions/core/allOf/2/properties/id",
                },
                "collection": {
                    "description": "Collection",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "item.json#/collection",
                },
                "geometry": {
                    "description": "Geometry",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/geometry",
                },
                "datetime": {
                    "description": "Datetime",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/"
                    "datetime.json#/properties/datetime",
                },
            },
            "additionalProperties": True,
        }
    )


@bp.route("/collections/<collection>")
def collection(collection: str):
    """
    Overview of a WFS Collection (a datacube product)
    """
    return _stac_response(_stac_collection(collection))


@bp.route("/collections/<collection>/items")
def collection_items(collection: str):
    """
    We no longer have one 'items' link. Redirect them to a stac search that implements the
    same FeatureCollection result.
    """
    try:
        _model.STORE.get_dataset_type(collection)
    except KeyError:
        abort(404, f"Product {collection!r} not found")

    return flask.redirect(
        url_for(".stac_search", collection=collection, **request.args)
    )


@bp.route("/collections/<collection>/items/<uuid:dataset_id>")
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


# Catalogs


@bp.route("/catalogs/<collection>/<int:year>-<int:month>")
def collection_month(collection: str, year: int, month: int):
    """ """
    all_time_summary = _model.get_time_summary(collection, year, month)
    if not all_time_summary:
        abort(404, f"No data for {collection!r} {year} {month}")

    request_args = request.args
    limit = request_args.get("limit", default=DEFAULT_CATALOG_SIZE, type=int)
    offset = request_args.get("_o", default=0, type=int)

    items = list(
        _model.STORE.search_items(
            product_names=[collection],
            time=_utils.as_time_range(year, month),
            limit=limit + 1,
            offset=offset,
            # We need the full datast to get dataset labels
            full_dataset=True,
        )
    )
    returned = items[:limit]
    there_are_more = len(items) == limit + 1

    optional_links: List[Link] = []
    if there_are_more:
        next_url = url_for(
            ".collection_month",
            collection=collection,
            year=year,
            month=month,
            _o=offset + limit,
        )
        optional_links.append(Link(rel="next", target=next_url))

    date = datetime(year, month, 1).date()
    c = Catalog(
        f"{collection}-{year}-{month}",
        description=f'{collection} for {date.strftime("%B %Y")}',
    )

    c.links.extend(
        [
            Link(rel="self", target=request.url),
            # dict(rel='parent', href= catalog?,
            # Each item.
            *(
                Link(
                    title=_utils.dataset_label(item_summary.odc_dataset),
                    rel="item",
                    target=url_for(
                        ".item",
                        collection=item_summary.product_name,
                        dataset_id=item_summary.dataset_id,
                    ),
                )
                for item_summary in items
            ),
            *optional_links,
        ]
    )

    # ????
    c.extra_fields["numberReturned"] = len(returned)
    c.extra_fields["numberMatched"] = all_time_summary.dataset_count

    return _stac_response(c)


@bp.route("/catalogs/arrivals")
def arrivals():
    """
    Virtual catalog of the items most recently indexed into this index
    """
    c = Catalog(
        id="arrivals",
        title="Dataset Arrivals",
        description="The most recently added Items to this index",
    )

    c.links.extend(
        [
            Link(rel="self", target=request.url),
            Link(
                rel="items",
                target=url_for(".arrivals_items"),
            ),
        ]
    )
    return _stac_response(c)


@bp.route("/catalogs/arrivals/items")
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
            include_total_count=False,
        )
    )


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
