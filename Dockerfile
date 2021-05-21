ARG V_BASE=3.3.0

FROM opendatacube/geobase-builder:${V_BASE} as env_builder

COPY requirements-docker.txt constraints-docker.txt /
RUN env-build-tool new /requirements-docker.txt /constraints-docker.txt /env

# Copy the environment in
FROM opendatacube/geobase-runner:${V_BASE}
COPY --from=env_builder /env /env
ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment

# Do the apt install process, including more recent Postgres/PostGIS
RUN apt-get update && apt-get install -y \
  make curl gnupg git build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install postgres client 11
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
  sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
  apt-get update && apt-get install -y \
  postgresql-client-11 \
  && rm -rf /var/lib/apt/lists/*

ENV PATH="/env/bin:${PATH}"

# Set up a nice workdir, and only copy the things we care about in
RUN mkdir -p /code
WORKDIR /code

ADD . /code

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
RUN if [ "$ENVIRONMENT" = "deployment" ] ; then FLAG='' ; else FLAG='-e'; fi \
  && /env/bin/pip install ${FLAG} .[${ENVIRONMENT}] \
  && rm -rf $HOME/.cache/pip

# Delete code from the /code folder if we're in a prod build
RUN if [ "$ENVIRONMENT" = "deployment" ]; then rm -rf /code/*; fi

# This is for prod, and serves as docs. It's usually overwritten
CMD gunicorn -b '0.0.0.0:8080' -w 1 '--worker-class=egg:meinheld#gunicorn_worker'  --timeout 60 --config python:cubedash.gunicorn_config cubedash:app
