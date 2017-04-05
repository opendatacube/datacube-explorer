# AGDC Operations Dashboard [![Build Status](https://travis-ci.org/data-cube/agdc-v2-dashboard.svg?branch=develop)](https://travis-ci.org/data-cube/agdc-v2-dashboard)

## Setup

Use of a [Data Cube conda environment](https://datacube-core.readthedocs.io/en/latest/ops/conda.html)
is recommended for install.

After setup of your environment, install the dashboard dependencies:

    pip install -r requirements.txt

Then run the app using a typical python wsgi server, for example:

    pip install gunicorn
    gunicorn -b '127.0.0.1:8080' -w 5 --timeout 300 cubedash:app

Convenience scripts are available for running in development with hot-reload (`./run-dev.sh`)
or gunicorn (`./run.sh`).

Note that most data is slow when loaded for the first time, but is cached for subsequent requests.

## NCI Usage

A dashboard install is available from VDI on the NCI:

    module use /g/data/v10/public/modules/modulefiles
    module load agdc-py3-prod
    /g/data/v10/public/run-dash.sh

Then open the given link in your VDI web browser.
