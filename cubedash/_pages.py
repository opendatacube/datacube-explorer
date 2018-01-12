import itertools

import flask

import datacube
from datacube.model import DatasetType

from . import _api, _dataset, _filters, _model, _platform, _product

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_product.bp)


@app.route("/about")
def about_page():
    return flask.render_template("about.html", datacube_version=datacube.__version__)


@app.context_processor
def inject_product_list():
    types = sorted(list(_model.index.datasets.types.get_all()), key=_get_product_group)

    # Group by platform
    platform_products = dict(
        (name or "", list(items))
        for (name, items) in itertools.groupby(types, key=_get_product_group)
    )

    return dict(products=types, platform_products=platform_products)


def _get_product_group(dt: DatasetType):
    group: str = dt.fields.get("product_type")
    if not group:
        return "Misc"

    return group.replace("_", " ")


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(
        flask.url_for("product.spatial_page", product_name="ls7_level1_scene")
    )
