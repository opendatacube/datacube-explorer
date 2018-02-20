#!/usr/bin/env bash

set -eu

log_dir="/var/log/nginx"
out_file="/var/www/dea-dashboard/cubedash/static/access.html"

zcat "${log_dir}/"access.log*.gz | /bin/goaccess --log-format=COMBINED -a -o "${out_file}" "${log_dir}/access.log"  -
