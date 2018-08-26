import functools
import inspect
import itertools
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Counter, Dict, Optional

import fiona
import flask
import pyproj
import shapely
import shapely.prepared
import shapely.wkb
import structlog
from flask import abort, redirect, request, url_for
from shapely.geometry import MultiPolygon, shape
from shapely.ops import transform
from sqlalchemy import event
from werkzeug.datastructures import MultiDict

import cubedash
import datacube
from datacube.model import DatasetType
from datacube.scripts.dataset import build_dataset_info

from . import _api, _dataset, _filters, _model, _platform, _product
from . import _utils as utils
from ._utils import alchemy_engine, as_json

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_product.bp)
app.register_blueprint(_platform.bp)

_LOG = structlog.getLogger()

_HARD_SEARCH_LIMIT = 500
_WRS_PATH_ROW = (
    Path(__file__).parent / "data" / "WRS2_descending" / "WRS2_descending.shp"
)


# @app.route('/')
@app.route("/<product_name>")
@app.route("/<product_name>/<int:year>")
@app.route("/<product_name>/<int:year>/<int:month>")
@app.route("/<product_name>/<int:year>/<int:month>/<int:day>")
def overview_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    product, product_summary, selected_summary = _load_product(
        product_name, year, month, day
    )

    datasets = None
    regions = None

    footprint_wrs84 = None

    if selected_summary and selected_summary.dataset_count:
        if selected_summary.footprint_geometry:
            start = time.time()
            from_crs = pyproj.Proj(init=selected_summary.footprint_crs)
            to_crs = pyproj.Proj(init="epsg:4326")
            footprint_wrs84 = transform(
                lambda x, y: pyproj.transform(from_crs, to_crs, x, y),
                selected_summary.footprint_geometry,
            )
            _LOG.info(
                "overview.footprint_size_diff",
                from_len=len(selected_summary.footprint_geometry.wkt),
                to_len=len(footprint_wrs84.wkt),
            )
            _LOG.debug("overview.footprint_proj", time_sec=time.time() - start)

        # The per-dataset view is less useful now that we show grids separately.
        # datasets = None if selected_summary.dataset_count > 1000
        # else get_datasets_geojson(product_name, year, month, day)

        start = time.time()
        regions = (
            get_region_counts(
                selected_summary.region_dataset_counts, footprint_wrs84, product
            )
            if selected_summary.region_dataset_counts
            else None
        )

        _LOG.debug("overview.region_gen", time_sec=time.time() - start)

    return flask.render_template(
        "overview.html",
        year=year,
        month=month,
        day=day,
        regions_geojson=regions,
        datasets_geojson=datasets,
        product=product,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
        footprint_wrs84=footprint_wrs84,
    )


def get_region_counts(
    region_counts: Counter[str], footprint: MultiPolygon, product: DatasetType
) -> Optional[Dict]:
    region_geometry = _region_geometry_function(product, footprint)
    if not region_geometry:
        return None

    low, high = min(region_counts.values()), max(region_counts.values())
    return {
        "type": "FeatureCollection",
        "properties": {"region_item_name": "Tile", "min_count": low, "max_count": high},
        "features": [
            {
                "type": "Feature",
                "geometry": region_geometry(region_code).__geo_interface__,
                "properties": {
                    "region_code": region_code,
                    "count": region_counts[region_code],
                },
            }
            for region_code in region_counts
        ],
    }


def _from_xy_region_code(region_code: str):
    x, y = region_code.split("_")
    return int(x), int(y)


def _region_geometry_function(product, footprint):
    grid_spec = product.grid_spec
    md_fields = product.metadata_type.dataset_fields
    # TODO: Geometry for other types of regions (path/row, MGRS)

    # hltc has a grid spec, but most attributes are missing, so grid_spec functions fail.
    # Therefore: only assume there's a grid if tile_size is specified. TODO: Is the product wrong?
    if grid_spec and grid_spec.tile_size:

        def region_geometry(region_code: str) -> shapely.geometry.GeometryCollection:
            """
            Get a whole polygon for a gridcell
            """
            extent = grid_spec.tile_geobox(
                _from_xy_region_code(region_code)
            ).geographic_extent
            # TODO: The ODC Geometry __geo_interface__ breaks for some products
            # (eg, when the inner type is a GeometryCollection?)
            # So we're now converting to shapely to do it.
            # TODO: Is there a nicer way to do this?
            # pylint: disable=protected-access
            shapely_extent = shapely.wkb.loads(extent._geom.ExportToWkb())

            return shapely_extent

    elif "sat_path" in md_fields:
        path_row_shapes = _get_path_row_shapes()

        def region_geometry(region_code: str) -> shapely.geometry.GeometryCollection:
            return path_row_shapes[_from_xy_region_code(region_code)]

    else:
        _LOG.info("region.geom.unknown", product_name=product.name)
        return None

    if footprint is None:
        return region_geometry
    else:
        footprint_boundary = shapely.prepared.prep(footprint.boundary)

        def region_geometry_cut(
            region_code: str
        ) -> shapely.geometry.GeometryCollection:
            """
            Cut the polygon down to the footprint
            """
            shapely_extent = region_geometry(region_code)

            # We only need to cut up tiles that touch the edges of the footprint (including inner "holes")
            # Checking the boundary is ~2.5x faster than running intersection() blindly, from my tests.
            if footprint_boundary.intersects(shapely_extent):
                return footprint.intersection(shapely_extent)
            else:
                return shapely_extent

        return region_geometry_cut


@functools.lru_cache()
def _get_path_row_shapes():
    path_row_shapes = {}
    with fiona.open(str(_WRS_PATH_ROW)) as f:
        for k, item in f.items():
            prop = item["properties"]
            path_row_shapes[prop["PATH"], prop["ROW"]] = shape(item["geometry"])
    return path_row_shapes


# @app.route('/datasets')
@app.route("/datasets/<product_name>")
@app.route("/datasets/<product_name>/<int:year>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>")
@app.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def search_page(
    product_name: str = None, year: int = None, month: int = None, day: int = None
):
    product, product_summary, selected_summary = _load_product(
        product_name, year, month, day
    )
    time_range = utils.as_time_range(year, month, day)

    args = MultiDict(flask.request.args)
    query = utils.query_to_search(args, product=product)

    # Always add time range, selected product to query
    if product_name:
        query["product"] = product_name
    if time_range:
        query["time"] = time_range

    _LOG.info("query", query=query)

    # TODO: Add sort option to index API
    datasets = sorted(
        _model.STORE.index.datasets.search(**query, limit=_HARD_SEARCH_LIMIT),
        key=lambda d: d.center_time,
    )

    if request_wants_json():
        return as_json(
            dict(datasets=[build_dataset_info(_model.STORE.index, d) for d in datasets])
        )
    return flask.render_template(
        "search.html",
        year=year,
        month=month,
        day=day,
        product=product,
        # Summary for the whole product
        product_summary=product_summary,
        # Summary for the users' currently selected filters.
        selected_summary=selected_summary,
        datasets=datasets,
        query_params=query,
        result_limit=_HARD_SEARCH_LIMIT,
    )


@app.route("/<product_name>/spatial")
def spatial_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


@app.route("/<product_name>/timeline")
def timeline_page(product_name: str):
    """Legacy redirect to maintain old bookmarks"""
    return redirect(url_for("overview_page", product_name=product_name))


def _load_product(product_name, year, month, day):
    product = None
    if product_name:
        product = _model.STORE.index.products.get_by_name(product_name)
        if not product:
            abort(404, "Unknown product %r" % product_name)

    # Entire summary for the product.
    product_summary = _model.get_summary(product_name)
    selected_summary = _model.get_summary(product_name, year, month, day)

    return product, product_summary, selected_summary


def request_wants_json():
    best = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return (
        best == "application/json"
        and request.accept_mimetypes[best] > request.accept_mimetypes["text/html"]
    )


@app.route("/about")
def about_page():
    return flask.render_template("about.html")


@app.context_processor
def inject_globals():
    product_summaries = _model.get_products_with_summaries()

    # Group by product type
    def key(t):
        return t[0].fields.get("product_type")

    grouped_product_summarise = sorted(
        (
            (name or "", list(items))
            for (name, items) in itertools.groupby(
                sorted(product_summaries, key=key), key=key
            )
        ),
        # Show largest groups first
        key=lambda k: len(k[1]),
        reverse=True,
    )

    return dict(
        products=product_summaries,
        grouped_products=grouped_product_summarise,
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
        app_version=cubedash.__version__,
        last_updated_time=_model.get_last_updated(),
    )


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    available_product_names = [p.name for p, _ in _model.get_products_with_summaries()]

    for product_name in _model.DEFAULT_START_PAGE_PRODUCTS:
        if product_name in available_product_names:
            default_product = product_name
            break
    else:
        default_product = available_product_names[0]

    return flask.redirect(flask.url_for("overview_page", product_name=default_product))


# Add server timings to http headers.
if app.debug:

    @app.before_request
    def time_start():
        flask.g.start_render = time.time()
        flask.g.datacube_query_time = 0
        flask.g.datacube_query_count = 0

    @event.listens_for(alchemy_engine(_model.STORE.index), "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        conn.info.setdefault("query_start_time", []).append(time.time())

    @event.listens_for(alchemy_engine(_model.STORE.index), "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        flask.g.datacube_query_time += time.time() - conn.info["query_start_time"].pop(
            -1
        )
        flask.g.datacube_query_count += 1
        # print(f"===== {flask.g.datacube_query_time*1000} ===: {repr(statement)}")

    @app.after_request
    def time_end(response: flask.Response):
        render_time = time.time() - flask.g.start_render
        response.headers.add_header(
            "Server-Timing",
            f"app;dur={render_time*1000},"
            f'odcquery;dur={flask.g.datacube_query_time*1000};desc="ODC query time",'
            f"odcquerycount_{flask.g.datacube_query_count};"
            f'desc="{flask.g.datacube_query_count} ODC queries"',
        )
        return response

    def decorate_all_methods(cls, decorator):
        """
        Decorate all public methods of the class with the given decorator.
        """
        for name, clasification, clz, attr in inspect.classify_class_attrs(cls):
            if clasification == "method" and not name.startswith("_"):
                setattr(cls, name, decorator(attr))
        return cls

    def print_datacube_query_times():
        from click import style

        def with_timings(function):
            """
            Decorate the given function with a stderr print of timing
            """

            @functools.wraps(function)
            def decorator(*args, **kwargs):
                start_time = time.time()
                ret = function(*args, **kwargs)
                duration_secs = time.time() - start_time
                print(
                    f"== Index Call == {style(function.__name__, bold=True)}: "
                    f"{duration_secs*1000}",
                    file=sys.stderr,
                    flush=True,
                )
                return ret

            return decorator

        # Print call time for all db layer calls.
        import datacube.drivers.postgres._api as api

        decorate_all_methods(api.PostgresDbAPI, with_timings)

    print_datacube_query_times()
