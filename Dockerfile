FROM ubuntu:bionic

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget gnupg
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
    apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" \
    >> /etc/apt/sources.list.d/postgresql.list

ADD requirements-apt.txt /tmp/
RUN apt-get update \
    && sed 's/#.*//' /tmp/requirements-apt.txt | xargs apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal && \
    export C_INCLUDE_PATH=/usr/include/gdal && \
    export GDAL_DATA="$(gdal-config --datadir)" && \
    pip3 install GDAL==$(gdal-config --version)

ADD requirements.txt /tmp/
RUN pip3 install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt \
    && rm -rf $HOME/.cache/pip

RUN mkdir -p /code
WORKDIR /code

ADD setup.py setup.cfg pyproject.toml /code/
ADD cubedash /code/cubedash
ADD .git /code/.git

RUN pip3 install --upgrade --extra-index-url \
    https://packages.dea.ga.gov.au/ 'datacube' 'digitalearthau'

RUN pip3 install .[deployment]

CMD gunicorn -b '0.0.0.0:8080' -w 1 '--worker-class=egg:meinheld#gunicorn_worker'  --timeout 60 cubedash:app
