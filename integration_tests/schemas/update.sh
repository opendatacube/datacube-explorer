#!/usr/bin/env bash

set -eu

get="curl -s -O"

$get 'http://geojson.org/schema/FeatureCollection.json'

cd stac
$get 'https://raw.githubusercontent.com/radiantearth/stac-spec/master/item-spec/json-schema/item.json'
$get 'https://raw.githubusercontent.com/radiantearth/stac-spec/master/item-spec/json-schema/geojson.json'
$get 'https://raw.githubusercontent.com/radiantearth/stac-spec/master/catalog-spec/json-schema/catalog.json'

echo "Succeess"
echo "If git status shows any changes, rerun tests, and commit them"
