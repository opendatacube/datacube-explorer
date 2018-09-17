#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

pylint -j 2 --reports no cubedash

python -m pytest -r sx --durations=5 "$@"

