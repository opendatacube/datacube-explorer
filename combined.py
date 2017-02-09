import sys
from cubedash import app
from werkzeug.serving import run_simple
from datacube_apps.wms_wsgi import application as backend
from werkzeug.wsgi import DispatcherMiddleware


if __name__ == '__main__':
    DEBUG_MODE = len(sys.argv) == 2 and sys.argv[1] == '--debug'
    app.jinja_env.auto_reload = DEBUG_MODE
    # app.run(port=8080, debug=DEBUG_MODE)

    application = DispatcherMiddleware(app, {
        '/agdc_wms': backend
    })
    run_simple('localhost', 5000, application,
               use_reloader=True, use_debugger=True, use_evalex=True)
