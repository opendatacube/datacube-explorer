FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.1

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
      vim \
      nano \
      tini \
      wget \
      postgresql-client \
      python3-pip \
      # For Psycopg2
      libpq-dev python3-dev \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}


# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment
# ARG ENVIRONMENT=test

RUN echo "Environment is: $ENVIRONMENT" && \
    pip install pip-tools pytest-cov

# Set up a nice workdir and add the live code
ENV APPDIR=/code
WORKDIR $APPDIR
COPY . $APPDIR

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
RUN if [ "$ENVIRONMENT" = "deployment" ] ; then\
        pip install .[$ENVIRONMENT]; \
        rm -rf /code/* /code/.git* ; \
    else \
        pip install --editable .[$ENVIRONMENT]; \
    fi && \
    pip freeze

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
     "cubedash:app"]
