# syntax=docker/dockerfile:1
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.3@sha256:a7c6f68b9868420861be6dd51873ac464fc587ae3b6206b546408d67d697328e AS base

LABEL org.opencontainers.image.source=https://github.com/opendatacube/datacube-explorer
LABEL org.opencontainers.image.description="Datacube Explorer"
LABEL org.opencontainers.image.licences="Apache-2.0"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1

FROM base AS builder

ARG UV=https://github.com/astral-sh/uv/releases/download/0.8.4/uv-x86_64-unknown-linux-gnu.tar.gz

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

ENV UV_COMPILE_BYTECODE=0 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.12

WORKDIR /build

ADD --checksum=sha256:eb61d39fdc6ea21a6d00a24b50376102168240849c5022d3eba331f972ba3934 --chown=root:root --chmod=644 --link $UV uv.tar.gz

RUN tar xf uv.tar.gz -C /usr/local/bin --strip-components=1 --no-same-owner

COPY --link pyproject.toml uv.lock /build/

# Use a separate cache volume for uv on opendatacube projects, so it is
# not inseparable from pip/poetry/npm/etc. cache stored in /root/.cache.
RUN --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    uv sync --frozen --extra=deployment --no-install-project \
      --no-binary-package fiona \
      --no-binary-package netcdf4 \
      --no-binary-package pyproj \
      --no-binary-package psycopg2 \
      --no-binary-package rasterio \
      --no-binary-package shapely

COPY --link . /build/

ARG ENVIRONMENT=deployment
RUN --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    EXTRAS=$( ([ "$ENVIRONMENT" = "deployment" ] && echo "--extra=deployment --no-dev") || \
                 echo "--extra=test") \
    && uv sync --frozen $EXTRAS --no-editable

FROM base

# Add login-script for UID/GID-remapping.
COPY --chown=root:root --link docker/files/remap-user.sh /usr/local/bin/remap-user.sh

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
            gosu \
            # For .docker/create_db.sh.
            postgresql-client \
            tini \
    && mkdir /app \
    && chown ubuntu:ubuntu /app

COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

# Environment is test or deployment.
ARG ENVIRONMENT=deployment
RUN ([ "$ENVIRONMENT" != "deployment" ] || \
           rm -f /usr/local/bin/uv*)

COPY --from=builder --link --chown=1000:1000 /app /app

# Configure user
WORKDIR "/home/ubuntu"

ENV PATH=/app/bin:$PATH

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
