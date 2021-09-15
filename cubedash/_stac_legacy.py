"""
Legacy redirects to maintain old stac api URLs
"""
import json

import flask
from flask import Response, request, url_for
from werkzeug.urls import iri_to_uri

bp = flask.Blueprint("stac_legacy", __name__)


@bp.route("/collections/<collection>")
def legacy_collection(collection: str):
    """Legacy redirect for non-stac prefixed offset"""
    return legacy_redirect(
        url_for("stac.collection", collection=collection, **request.args)
    )


@bp.route("/collections/<collection>/items")
def legacy_collection_items(collection: str):
    """Legacy redirect for non-stac prefixed offset"""
    return legacy_redirect(
        url_for("stac.collection_items", collection=collection, **request.args)
    )


@bp.route("/collections/<collection>/items/<dataset_id>")
def legacy_item(collection, dataset_id):
    """Legacy redirect for non-stac prefixed offset"""
    return legacy_redirect(
        url_for(
            "stac.item", collection=collection, dataset_id=dataset_id, **request.args
        )
    )


def legacy_redirect(location):
    """
    Redirect to a new location.

    Used for backwards compatibility with older URLs that may be bookmarked or stored.
    """
    if isinstance(location, str):
        location = iri_to_uri(location, safe_conversion=True)
    response = Response(
        json.dumps(
            {
                "code": 302,
                "name": "legacy-redirect",
                "description": "This is a legacy URL endpoint -- please follow the redirect "
                "and update links to the new one",
                "new_location": location,
            }
        ),
        302,
        mimetype="application/json",
    )
    response.headers["Location"] = location
    return response
