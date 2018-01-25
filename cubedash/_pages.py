import flask
import itertools

import datacube
from datacube.model import DatasetType
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
        'about.html',
        datacube_version=datacube.__version__
    )


@app.context_processor
def inject_product_list():
    types = _model.list_products()

    # Group by product type
    platform_products = dict(
        (name or '', list(items))
        for (name, items) in itertools.groupby(types, key=lambda t: t[0].fields.get('product_type'))
    )

    return dict(
        products=types,
        platform_products=platform_products
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
