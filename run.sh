#!/usr/bin/env bash

gunicorn \
    -w 5 \
    --timeout 300 \
    --worker-class="egg:meinheld#gunicorn_worker" \
    cubedash:app

