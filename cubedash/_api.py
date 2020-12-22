import logging

from flask import Blueprint, abort, request

from cubedash import _utils

from . import _model
from ._utils import as_geojson
from .summary import ItemSort

_MAX_DATASET_RETURN = 2000

_LOG = logging.getLogger(__name__)
bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/datasets/<product_name>")
@bp.route("/datasets/<product_name>/<int:year>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def datasets_geojson(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    limit = request.args.get("limit", default=500, type=int)
    if limit > _MAX_DATASET_RETURN:
        limit = _MAX_DATASET_RETURN

    time = _utils.as_time_range(year, month, day, tzinfo=_model.STORE.grouping_timezone)

    return as_geojson(
        dict(
            type="FeatureCollection",
            features=[
                s.as_geojson()
                for s in _model.STORE.search_items(
                    product_names=[product_name],
                    time=time,
                    limit=limit,
                    order=ItemSort.UNSORTED,
                )
                if s.geom_geojson is not None
            ],
        )
    )

    # TODO: replace this api with stac?
    #       Stac includes much more information in records, so has to join the
    #       dataset table, so is slower, but does it matter?
    # Can trivially redirect to stac as its return value is still geojson:
    # return flask.redirect(
    #     flask.url_for(
    #         'stac.stac_search',
    #         product_name=product_name,
    #         time=_unparse_time_range(time) if time else None,
    #         limit=limit,
    #     )
    # )


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
