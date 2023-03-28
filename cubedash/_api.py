import logging
from datetime import date, datetime

import flask
from flask import Blueprint, abort, request

from cubedash import _utils

from . import _model
from ._utils import as_geojson, as_json
from .summary import ItemSort

_LOG = logging.getLogger(__name__)
bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/datasets/<product_name>")
@bp.route("/datasets/<product_name>/<int:year>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def datasets_geojson(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    limit = request.args.get(
        "limit",
        default=flask.current_app.config["CUBEDASH_DEFAULT_API_LIMIT"],
        type=int,
    )
    hard_limit = flask.current_app.config["CUBEDASH_HARD_API_LIMIT"]
    if limit > hard_limit:
        limit = hard_limit

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
        ),
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
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
    return as_geojson(
        _model.get_footprint_geojson(product_name, year, month, day),
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
    )


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
    return as_geojson(
        regions, downloadable_filename_prefix=_utils.api_path_as_filename_prefix()
    )


@bp.route("/dataset-timeline/<product_name>")
@bp.route("/dataset-timeline/<product_name>/<int:year>")
@bp.route("/dataset-timeline/<product_name>/<int:year>/<int:month>")
@bp.route("/dataset-timeline/<product_name>/<int:year>/<int:month>/<int:day>")
def dataset_timeline(
    product_name: str, year: int = None, month: int = None, day: int = None
):
    summary = _model.get_time_summary(product_name, year, month, day)
    if summary is None:
        abort(
            404,
            f"No known information for product "
            f"{product_name!r} {year or 'all'} {month or 'all'} {day or 'all'}",
        )

    def _datekey(k):
        # The timezone is the global grouping timezone: we don't want it in json.
        if type(k) is date:
            k = datetime(k.year, k.month, k.day)
        return k.replace(tzinfo=None).isoformat()

    return as_json(
        {_datekey(k): v for k, v in summary.timeline_dataset_counts.items()},
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
    )
