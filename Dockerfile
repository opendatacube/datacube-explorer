FROM ubuntu:bionic

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment
RUN echo "Environment is: $ENVIRONMENT"

# Do the apt install process, including more recent Postgres/PostGIS
RUN apt-get update && apt-get install -y wget gnupg \
    && rm -rf /var/lib/apt/lists/*
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
    apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" \
    >> /etc/apt/sources.list.d/postgresql.list

ADD requirements-apt.txt /tmp/
RUN apt-get update \
    && sed 's/#.*//' /tmp/requirements-apt.txt | xargs apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# GDAL is particular...
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal \
    && export C_INCLUDE_PATH=/usr/include/gdal \
    && export GDAL_DATA="$(gdal-config --datadir)" \
    && pip3 install GDAL==$(gdal-config --version) \
    && rm -rf $HOME/.cache/pip

# Install some important dependencies
RUN pip3 install --upgrade pip setuptools \
    && pip3 install --extra-index-url \
    https://packages.dea.ga.gov.au/ 'datacube' 'digitalearthau' \
    && rm -rf $HOME/.cache/pip

# Set up a nice workdir, and only copy the things we care about in
RUN mkdir -p /code
WORKDIR /code

ADD . /code

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
RUN if [ "$ENVIRONMENT" = "deployment" ] ; then FLAG='' ; else FLAG='-e'; fi \
    && pip3 install ${FLAG} .[${ENVIRONMENT}] \
    && rm -rf $HOME/.cache/pip

# Delete code from the /code folder if we're in a prod build
RUN if [ "$ENVIRONMENT" = "deployment" ]; then rm -rf /code/*; fi

# This is for prod, and serves as docs. It's usually overwritten
CMD gunicorn -b '0.0.0.0:8080' -w 1 '--worker-class=egg:meinheld#gunicorn_worker'  --timeout 60 cubedash:app
