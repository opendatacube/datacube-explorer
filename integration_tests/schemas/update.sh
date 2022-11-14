#!/usr/bin/env bash

set -eu


stac_tag='v1.0.0'
stac_api_tag='master'


function get() {
    echo "$1"
    wget -r "$1"
}

get 'http://geojson.org/schema/Geometry.json'
get 'http://geojson.org/schema/FeatureCollection.json'
get 'http://geojson.org/schema/Feature.json'


# strip the 'v' from the start if there.
stac_version="${stac_tag#v}"
subfolder="stac-spec-${stac_version}"

set -x

wget https://github.com/radiantearth/stac-spec/archive/${stac_tag}.tar.gz
tar -xvf ${stac_tag}.tar.gz --wildcards "${subfolder}/*/json-schema/*.json"
rm ${stac_tag}.tar.gz
rm -rf "schemas.stacspec.org/${stac_version}"
mv ${subfolder} "schemas.stacspec.org/${stac_version}"
rm -rf "stac/${stac_version}"
ln -s "$(pwd)/schemas.stacspec.org/${stac_version}" "$(pwd)/stac"

api_subfolder="stac-api-spec-${stac_api_tag}"
wget https://github.com/radiantearth/stac-api-spec/archive/${stac_api_tag}.tar.gz
tar -xvf ${stac_api_tag}.tar.gz --wildcards "${api_subfolder}/*/json-schema/*.json"
rm ${stac_api_tag}.tar.gz
rm -rf "schemas.stacspec.org/${stac_api_tag}"
mv ${api_subfolder} "schemas.stacspec.org/${stac_api_tag}"
rm -rf "stac-api/${stac_api_tag}"
ln -s "$(pwd)/schemas.stacspec.org/${stac_api_tag}" "$(pwd)/stac-api"

# The ItemCollection was removed from core stac, but is used by stac-api.
cd "stac/${stac_version}/item-spec/json-schema"
wget https://raw.githubusercontent.com/radiantearth/stac-spec/568a04821935cc92de7b4b05ea6fa9f6bf8a0592/item-spec/json-schema/itemcollection.json

echo "Success"
echo "If git status shows any changes, rerun tests, and commit them"
