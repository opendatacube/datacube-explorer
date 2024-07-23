FROM ghcr.io/osgeo/gdal:ubuntu-small-latest as builder

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONFAULTHANDLER=1

# Apt installation
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      # For Psycopg2
      libpq-dev \
      python3-dev \
      python3-pip

WORKDIR /build

RUN python3 -m pip --disable-pip-version-check -q wheel --no-binary psycopg2 psycopg2

FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONFAULTHANDLER=1

# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment
# ARG ENVIRONMENT=test

# Apt installation
# git: required by setuptools_scm.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      git \
      # For Psycopg2
      libpq5 \
      tini \
      postgresql-client \
      python3-pip \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log} && \
    echo "Environment is: $ENVIRONMENT" && \
    ([ "$ENVIRONMENT" = "deployment" ] || \
      pip install --disable-pip-version-check pip-tools pytest-cov)

# Set up a nice workdir and add the live code
ENV APPDIR=/code
WORKDIR $APPDIR
COPY . $APPDIR

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
COPY --from=builder --link /build/*.whl ./
RUN python3 -m pip --disable-pip-version-check -q install --break-system-packages *.whl && \
    rm *.whl && \
    ([ "$ENVIRONMENT" = "deployment" ] || \
        pip --disable-pip-version-check install --editable --break-system-packages .[$ENVIRONMENT]) && \
    ([ "$ENVIRONMENT" != "deployment" ] || \
        (pip --no-cache-dir --disable-pip-version-check install --break-system-packages .[$ENVIRONMENT] && \
         rm -rf /code/* /code/.git*)) && \
    pip freeze && \
    ([ "$ENVIRONMENT" != "deployment" ] || \
        apt-get remove -y \
            git \
            git-man \
            python3-pip)

ENTRYPOINT ["/bin/tini", "--"]

# This is for prod, and serves as docs. It's usually overwritten
CMD ["gunicorn", \
     "-b", \
     "0.0.0.0:8080", \
     "-w", \
     "3", \
     "--threads=2", \
     "-k", \
     "gthread", \
     "--timeout", \
     "90", \
     "--config", \
     "python:cubedash.gunicorn_config", \
     "cubedash:create_app()"]
