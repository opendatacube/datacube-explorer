import itertools

import flask
from datetime import datetime

import datacube
from . import _filters, _dataset, _platform, _product, _api, _model

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_product.bp)


@app.route('/about')
def about_page():
    return flask.render_template(
        'about.html'
    )


@app.context_processor
def inject_globals():
    product_summaries = _model.list_product_summaries()

    # Group by product type
    def key(t):
        return t[0].fields.get('product_type')

    grouped_product_summarise = sorted(
        (
            (name or '', list(items))
            for (name, items) in
            itertools.groupby(sorted(product_summaries, key=key), key=key)
        ),
        # Show largest groups first
        key=lambda k: len(k[1]), reverse=True
    )

    return dict(
        products=product_summaries,
        grouped_products=grouped_product_summarise,
        current_time=datetime.utcnow(),
        datacube_version=datacube.__version__,
    )


@app.route('/')
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(
        flask.url_for(
            'product.spatial_page',
            product_name='ls7_nbar_scene'
        )
    )
