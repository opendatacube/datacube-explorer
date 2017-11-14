import sys

from ._pages import app

if __name__ == '__main__':
    DEBUG_MODE = len(sys.argv) == 2 and sys.argv[1] == '--debug'
    app.jinja_env.auto_reload = DEBUG_MODE
    app.run(port=8080, debug=DEBUG_MODE)
