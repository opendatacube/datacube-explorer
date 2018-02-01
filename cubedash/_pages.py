import itertools
from datetime import datetime

import flask

import datacube

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
def inject_globals():
    product_summaries = _model.list_product_summaries()

    # Group by product type
    grouped_product_summarise = dict(
        (name or "", list(items))
        for (name, items) in itertools.groupby(
            product_summaries, key=lambda t: t[0].fields.get("product_type")
        )
    )

    return dict(
        products=product_summaries,
        grouped_products=grouped_product_summarise,
        current_time=datetime.utcnow(),
    )


@app.route("/")
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(
        flask.url_for("product.spatial_page", product_name="ls7_nbar_scene")
    )
