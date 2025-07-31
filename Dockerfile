# syntax=docker/dockerfile:1
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.3@sha256:dab45abca3ca83695d442018692f4f8a0f41955871c57e6101d7f89a92375caa AS base

LABEL org.opencontainers.image.source=https://github.com/opendatacube/datacube-explorer
LABEL org.opencontainers.image.description="Datacube Explorer"
LABEL org.opencontainers.image.licences="Apache-2.0"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONFAULTHANDLER=1

FROM base AS builder

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        git \
        # For shapely with --no-binary.
        libgeos-dev \
        libhdf5-dev \
        libnetcdf-dev \
        libudunits2-dev \
        libproj-dev \
        # For psycopg2.
        libpq-dev \
        proj-bin \
        python3-dev

WORKDIR /build

RUN python3 -m pip --disable-pip-version-check -q wheel --no-binary psycopg2 psycopg2

FROM base

# Add login-script for UID/GID-remapping.
COPY --chown=root:root --link docker/files/remap-user.sh /usr/local/bin/remap-user.sh

# Apt installation
# git: required by setuptools_scm.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
            git \
            gosu \
            # For Psycopg2
            libpq5 \
            tini \
            postgresql-client \
            python3-dev \
            python3-pip

# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment
RUN echo "Environment is: $ENVIRONMENT" \
    ([ "$ENVIRONMENT" = "deployment" ] || \
        pip install --disable-pip-version-check pip-tools pytest-cov --break-system-packages)

# Set up a nice workdir and add the live code
ENV APPDIR=/code
WORKDIR $APPDIR
COPY . $APPDIR

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
COPY --from=builder --link /build/*.whl ./
RUN python3 -m pip --disable-pip-version-check -q install *.whl --break-system-packages && \
    rm *.whl && \
    ([ "$ENVIRONMENT" = "deployment" ] || \
        pip --disable-pip-version-check install --editable .[$ENVIRONMENT] --break-system-packages) && \
    ([ "$ENVIRONMENT" != "deployment" ] || \
        (pip --no-cache-dir --disable-pip-version-check install .[$ENVIRONMENT] --break-system-packages && \
         rm -rf /code/* /code/.git*)) && \
    pip freeze && \
    ([ "$ENVIRONMENT" != "deployment" ] || \
        apt-get remove -y \
            git \
            git-man \
            python3-pip)

ENTRYPOINT ["/usr/local/bin/remap-user.sh"]
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
