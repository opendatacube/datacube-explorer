import json
import logging

import flask
from flask import Blueprint, abort, request

from cubedash import _utils
from cubedash._stac import _unparse_time_range

from . import _model
from ._utils import as_geojson

_LOG = logging.getLogger(__name__)
bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/datasets/<product_name>")
@bp.route("/datasets/<product_name>/<int:year>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def datasets_geojson(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    bbox = None

    if "bbox" in request.args:
        bbox = json.loads(request.args["bbox"])

    limit = request.args.get("limit", type=int)

    time = _utils.as_time_range(year, month, day, tzinfo=_model.STORE.grouping_timezone)
    return flask.redirect(
        flask.url_for(
            "stac.stac_search",
            product_name=product_name,
            time=_unparse_time_range(time),
            bbox=bbox,
            limit=limit,
        )
    )


@bp.route("/footprint/<product_name>")
@bp.route("/footprint/<product_name>/<int:year>")
@bp.route("/footprint/<product_name>/<int:year>/<int:month>")
@bp.route("/footprint/<product_name>/<int:year>/<int:month>/<int:day>")
def footprint_geojson(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    return as_geojson(_model.get_footprint_geojson(product_name, year, month, day))


@bp.route("/regions/<product_name>")
@bp.route("/regions/<product_name>/<int:year>")
@bp.route("/regions/<product_name>/<int:year>/<int:month>")
@bp.route("/regions/<product_name>/<int:year>/<int:month>/<int:day>")
def regions_geojson(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    regions = _model.get_regions_geojson(product_name, year, month, day)
    if regions is None:
        abort(404, f"{product_name} does not have regions")
    return as_geojson(regions)
