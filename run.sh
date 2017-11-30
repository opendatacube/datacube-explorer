#!/usr/bin/env bash

gunicorn \
    -w 3 \
    --timeout 300 \
    --worker-class="egg:meinheld#gunicorn_worker" \
    cubedash:app

