import flask

from . import _filters, _dataset, _platform, _product, _api, _model

app = _model.app
app.register_blueprint(_filters.bp)
app.register_blueprint(_api.bp)
app.register_blueprint(_dataset.bp)
app.register_blueprint(_platform.bp)
app.register_blueprint(_product.bp)


@app.route('/')
def default_redirect():
    """Redirect to default starting page."""
    return flask.redirect(flask.url_for('product.spatial_page', product_name='ls7_level1_scene'))
