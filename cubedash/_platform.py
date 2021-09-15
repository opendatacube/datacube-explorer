import logging

from flask import Blueprint, redirect, url_for

_LOG = logging.getLogger(__name__)
bp = Blueprint("platform", __name__, url_prefix="/platform")


@bp.route("/<platform_name>")
def platforms_page(platform_name):
    # Legacy platform page. Redirect to list of timelines in about page.
    return redirect(url_for("product.products_page"))
