FROM osgeo/gdal:ubuntu-small-3.3.2

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PYTHONFAULTHANDLER=1

# Apt installation
RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      vim \
      nano \
      tini \
      wget \
      postgresql-client \
      python3-pip \
      # For Psycopg2
      libpq-dev python-dev \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}


# Environment can be whatever is supported by setup.py
# so, either deployment, test
ARG ENVIRONMENT=deployment
# ARG ENVIRONMENT=test

RUN echo "Environment is: $ENVIRONMENT"

RUN pip install pip-tools pre-commit pytest-cov

# Pip installation
RUN mkdir -p /conf
COPY requirements-docker.txt constraints-docker.txt /conf/
RUN pip install -r /conf/requirements-docker.txt -c /conf/constraints-docker.txt


# Dev setup: run pre-commit once, so its virtualenv is built and cached.
#    We do this in a tmp repository, before copying our real code, as we
#    want this cached by Docker and not rebuilt every time code changes
COPY .pre-commit-config.yaml /conf/

RUN if [ "$ENVIRONMENT" = "test" ] ; then \
       mkdir -p ~/pre-commit \
       && cp /conf/.pre-commit-config.yaml ~/pre-commit \
       && cd ~/pre-commit \
       && git init \
       && pre-commit run \
       && rm -rf ~/pre-commit ; \
    fi


# Set up a nice workdir and add the live code
ENV APPDIR=/code
RUN mkdir -p $APPDIR
WORKDIR $APPDIR
ADD . $APPDIR

# These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
# then we want to link the source (with the -e flag) and if we're in prod, we
# want to delete the stuff in the /code folder to keep it simple.
RUN if [ "$ENVIRONMENT" = "deployment" ] ; then\
        pip install .[$ENVIRONMENT] ; \
        rm -rf /code/* ; \
    else \
        pip install --editable .[$ENVIRONMENT] ; \
    fi

RUN pip freeze

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
     "60", \
     "--config", \
     "python:cubedash.gunicorn_config", \
     "cubedash:app"]
