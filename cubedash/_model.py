from __future__ import absolute_import

from flask import jsonify, logging

from datacube.index import index_connect
from datacube.utils import jsonify_document

# Only do expensive queries "once a day"
# Enough time to last the remainder of the work day, but not enough to still be there the next morning
CACHE_LONG_TIMEOUT_SECS = 60 * 60 * 18


def as_json(o):
    return jsonify(jsonify_document(o))


# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking (hence validate=False).
index = index_connect(application_name="cubedash", validate_connection=False)
