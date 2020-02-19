FROM opendatacube/datacube-core:1.7

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3-fiona \
    python3-shapely \
    libpng-dev \
    wget \
    vim \
    unzip \
    postgresql-client \
    jq \
    awscli \
    curl \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip first: pip3 install --upgrade pip.
RUN pip3 install --upgrade pip \
    && rm -rf $HOME/.cache/pip

RUN pip3 install gunicorn flask pyorbital colorama sentry-sdk[flask] raven gevent \
    && rm -rf $HOME/.cache/pip

WORKDIR /code
RUN mkdir /code/product-summaries

ADD . .

COPY deployment/load-rds-db-dump.bash .
COPY deployment/load-nci-dump.bash .

RUN pip3 install .[deployment]

CMD gunicorn -b '0.0.0.0:8000' --workers=3 --threads=2 -k gevent --timeout 121 --pid gunicorn.pid --log-level info \
--worker-tmp-dir -env/dev/shm cubedash:app
