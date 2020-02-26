FROM opendatacube/datacube-core:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3-fiona \
    python3-shapely \
    libpng-dev \
#    postgresql-client \
    curl \
    libev-dev \
    gpg-agent \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

RUN apt-get update

RUN apt install -y postgresql-client-11

RUN pip3 install --upgrade pip \
    && pip3 install gunicorn flask pyorbital colorama sentry-sdk[flask] raven \
    && rm -rf $HOME/.cache/pip

WORKDIR /code
RUN mkdir /code/product-summaries

ADD . .

RUN pip3 install .[deployment]

CMD gunicorn -b '0.0.0.0:8080' -w 1 '--worker-class=egg:meinheld#gunicorn_worker'  --timeout 60 cubedash:app
