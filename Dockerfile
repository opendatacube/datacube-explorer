# syntax=docker/dockerfile:1
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1@sha256:836d1646ff9ab30d7db947c7b9690de55f34c2fd7cc5af6086a0854ccb378f59 AS base

LABEL org.opencontainers.image.source=https://github.com/opendatacube/datacube-explorer
LABEL org.opencontainers.image.description="Datacube Explorer"
LABEL org.opencontainers.image.licences="Apache-2.0"

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1

# This cannot be inlined below (e.g., COPY --from=...) because Dependabot does not support that syntax yet
FROM ghcr.io/astral-sh/uv:0.11.24@sha256:99ea34acedc870ba4ad11a1f540a1c04267c9f30aadc465a94406f52dfda2c36 AS uv

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
        # For psycopg2.
        libpq-dev \
        python3-dev

ENV UV_COMPILE_BYTECODE=0 \
    UV_HTTP_RETRIES=10 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.14

WORKDIR /build

COPY --link --from=uv /uv /uvx /usr/local/bin/

COPY --link pyproject.toml uv.lock /build/

# Use a separate cache volume for uv on opendatacube projects, so it is
# not inseparable from pip/poetry/npm/etc. cache stored in /root/.cache.
RUN --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    uv sync --frozen --extra=deployment --no-install-project \
      --no-binary-package fiona \
      --no-binary-package netcdf4 \
      --no-binary-package psycopg \
      --no-binary-package psycopg-c \
      --no-binary-package psycopg2 \
      --no-binary-package rasterio \
      --no-binary-package shapely

COPY --link . /build/

ARG ENVIRONMENT=deployment
ARG EXPLORER_VERSION=""
# The deployment image should not have binaries that aid an attacker to get their
# rootkit in place, and uv downloads over the network. There is no conditional
# copy in Docker, so truncate the uv binaries to 0 bytes to render them harmless
# in the resulting deployment image.
RUN --mount=type=cache,id=opendatacube-uv-cache,target=/root/.cache \
    EXTRAS=$( ([ "$ENVIRONMENT" = "deployment" ] && echo "--extra=deployment --no-dev") || \
                 echo "--extra=test") \
    && export SETUPTOOLS_SCM_PRETEND_VERSION="${EXPLORER_VERSION}" \
    && uv sync --frozen $EXTRAS --no-editable \
    && ([ "$ENVIRONMENT" != "deployment" ] || \
        (chmod 644 /usr/local/bin/uv* && \
         echo "" > /usr/local/bin/uv && \
         echo "" > /usr/local/bin/uvx))

FROM base

ARG EXPLORER_VERSION=""
LABEL org.opencontainers.image.version=${EXPLORER_VERSION}

# Add login-script for UID/GID-remapping.
COPY --chown=root:root --link docker/files/remap-user.sh /usr/local/bin/remap-user.sh

ARG ENVIRONMENT=deployment
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    export DEBIAN_FRONTEND=noninteractive \
    && EXTRAS=$( ([ "$ENVIRONMENT" = "deployment" ] && echo "") || \
                 echo "git") \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
            $EXTRAS \
            gosu \
            libpq5 \
            tini \
    && mkdir /app \
    && chown ubuntu:ubuntu /app

# In the "deployment" build, these `uv` binaries will be 0 bytes.
# In the "test" build they're the actual `uv` tools.
COPY --from=builder --link /usr/local/bin/uv* /usr/local/bin/

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
     "cubedash.startup_utils:create_app()"]
