# DEA Operations Dashboard [![Build Status](https://travis-ci.org/data-cube/dea-dashboard.svg?branch=develop)](https://travis-ci.org/data-cube/dea-dashboard)

 
## Developer Setup

*Note*: Example server deployment directions are in the [deployment folder](deployment/README.md). 
These directions are for running from a local folder.

Firstly, install Data Cube: use of a [Data Cube conda environment](https://datacube-core.readthedocs.io/en/latest/ops/conda.html)
is recommended.

Test that you can run `datacube system check`, and that it's connecting
to the correct instance.

### Dependencies

Now install the dashboard dependencies:

    # These two should come from conda if you're using it, not pypi
    conda install fiona shapely
    
    # Install dependencies
    python ./setup.py develop

### Summary generation

Cache some `year` and `month` summaries:

    mkdir product-summaries
    nohup python -m cubedash.generate --all &>> summary-gen.log &

(This can take a while the first time, depending on your datacube size. 
We're using `nohup .. &` to run in the background.)

### Run

Then run the app using a typical python wsgi server, for example:

    pip install gunicorn
    gunicorn -b '127.0.0.1:8080' -w 5 --timeout 300 cubedash:app

Convenience scripts are available for running in development with hot-reload (`./run-dev.sh`)
or gunicorn (`./run.sh`).

Products will begin appearing one-by-one as the summaries are generated in the background.
If impatient, you can manually navigate to a product using `/<product_name`. (Eg `/ls5_nbar_albers`) 

## NCI Usage

A dashboard install is available from VDI on the NCI:

    module use /g/data/v10/public/modules/modulefiles
    module load agdc-py3-prod
    /g/data/v10/public/run-dash.sh

Then open the given link in your VDI web browser.

## Stylesheets

The css is compiled from Sass. Run `make` to rebuild them after a change,
or use your editor to watch for changes (PyCharm will prompt to do so).

