import sys
from datetime import datetime
from json import dumps as jsonify

import flask
from cachetools.func import ttl_cache
from dateutil import tz

from cubedash import _utils as utils
from datacube.index import index_connect
from datacube.model import Range
from datacube.utils import jsonify_document
from datacube.utils.geometry import CRS

app = flask.Flask("cubedash")
app.register_blueprint(utils.bp)

# Only do expensive queries "once a day"
# Enough time to last the remainder of the work day, but not enough to still be there the next morning
CACHE_LONG_TIMEOUT_SECS = 60 * 60 * 18


def as_json(o):
    return jsonify(jsonify_document(o), indent=4)


# Thread and multiprocess safe.
# As long as we don't run queries (ie. open db connections) before forking (hence validate=False).
index = index_connect(application_name="cubedash", validate_connection=False)


def next_date(date):
    if date.month == 12:
        return datetime(date.year + 1, 1, 1)
    else:
        return datetime(date.year, date.month + 1, 1)


def dataset_to_feature(ds):
    return {
        "type": "Feature",
        "geometry": ds.extent.to_crs(CRS("EPSG:4326")).__geo_interface__,
        "properties": {"id": ds.id, "product": ds.type.name, "time": ds.center_time},
    }


@app.route("/api/datasets/<product>/<int:year>-<int:month>")
@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def datasets_as_features(product, year, month):
    start = datetime(year, month, 1)
    time = Range(start, next_date(start))
    datasets = index.datasets.search(product=product, time=time)
    return as_json(
        {
            "type": "FeatureCollection",
            "features": [dataset_to_feature(ds) for ds in datasets],
        }
    )


@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def _timeline_years(from_year, product):
    timeline = index.datasets.count_product_through_time(
        "1 month",
        product=product,
        time=Range(datetime(from_year, 1, 1, tzinfo=tz.tzutc()), datetime.utcnow()),
    )
    return list(timeline)


@ttl_cache(ttl=CACHE_LONG_TIMEOUT_SECS)
def _timelines_platform(platform):
    products = index.datasets.count_by_product_through_time(
        "1 month",
        platform=platform,
        time=Range(datetime(1986, 1, 1, tzinfo=tz.tzutc()), datetime.utcnow()),
    )
    return list(products)


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(
        flask.url_for("product_spatial_page", product="ls7_level1_scene")
    )


@app.route("/<product>/spatial")
def product_spatial_page(product):
    types = index.datasets.types.get_all()
    return flask.render_template(
        "spatial.html", products=[p.definition for p in types], selected_product=product
    )


@app.route("/<product>/timeline")
def product_timeline_page(product):
    return flask.render_template(
        "timeline.html",
        timeline=_timeline_years(1986, product),
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product,
    )


@app.route("/<product>/datasets")
def product_datasets_page(product):
    args = flask.request.args
    query = {"product": product}
    query.update(utils.parse_query(args))
    # TODO: Add sort option to index API
    datasets = sorted(index.datasets.search_eager(**query), key=lambda d: d.center_time)
    return flask.render_template(
        "datasets.html",
        products=[p.definition for p in index.datasets.types.get_all()],
        selected_product=product,
        datasets=datasets,
        query_params=query,
    )


@app.route("/platform/<platform>")
def platform_page(platform):
    return flask.render_template(
        "platform.html",
        product_counts=_timelines_platform(platform),
        products=[p.definition for p in index.datasets.types.get_all()],
        platform=platform,
    )


@app.route("/datasets/<uuid:id_>")
def dataset_page(id_):
    dataset = index.datasets.get(id_, include_sources=True)

    source_datasets = {
        type_: index.datasets.get(dataset_d["id"])
        for type_, dataset_d in dataset.metadata.sources.items()
    }

    ordered_metadata = utils.get_ordered_metadata(dataset.metadata_doc)

    return flask.render_template(
        "dataset.html",
        dataset=dataset,
        dataset_metadata=ordered_metadata,
        derived_datasets=index.datasets.get_derived(id_),
        source_datasets=source_datasets,
    )


if __name__ == "__main__":
    DEBUG_MODE = len(sys.argv) == 2 and sys.argv[1] == "--debug"
    app.jinja_env.auto_reload = DEBUG_MODE
    app.run(port=8080, debug=DEBUG_MODE)
