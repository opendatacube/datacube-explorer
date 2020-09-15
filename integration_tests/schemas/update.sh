#!/usr/bin/env bash

set -eu


stac_tag='v1.0.0-beta.2'


function get() {
    echo "$1"
    wget -r "$1"
}

get 'http://geojson.org/schema/FeatureCollection.json'
get 'http://geojson.org/schema/Feature.json'

# strip the 'v' from the start if there.
stac_version="${stac_tag#v}"
subfolder="stac-spec-${stac_version}"

wget https://github.com/radiantearth/stac-spec/archive/${stac_tag}.tar.gz
tar -xvf ${stac_tag}.tar.gz --wildcards "${subfolder}/*/json-schema/*.json"
rm ${stac_tag}.tar.gz
rm -rf stac
mv -v ${subfolder} stac

echo "Success"
echo "If git status shows any changes, rerun tests, and commit them"
