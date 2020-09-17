#!/usr/bin/env bash
# Run single threaded server with debugger & reloading enabled.
export FLASK_APP=cubedash
export FLASK_DEBUG=1
export PYTHONPATH=${PYTHONPATH}:`pwd`

python -m flask run -p 8080
