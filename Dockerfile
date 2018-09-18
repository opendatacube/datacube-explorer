FROM opendatacube/datacube-core:latest

RUN apt-get update && apt-get install -y \
    python3-fiona python3-shapely \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install gunicorn flask pyorbital colorama \
    && rm -rf $HOME/.cache/pip

WORKDIR /code
RUN mkdir /code/product-summaries

ADD . .

RUN python3 ./setup.py develop

CMD gunicorn -b '0.0.0.0:8080' -w 1 --timeout 60 cubedash:app

