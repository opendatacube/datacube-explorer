#!/usr/bin/env bash

gunicorn -b '0.0.0.0:8080' --log-level DEBUG -w 3 --timeout 300 cubedash:app
