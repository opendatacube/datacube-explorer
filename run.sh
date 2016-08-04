#!/usr/bin/env bash

gunicorn -b '127.0.0.1:8080' --log-level DEBUG -w 3 --timeout 300 cubedash:app
