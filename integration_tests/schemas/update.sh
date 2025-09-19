#!/usr/bin/env bash

set -eu


stac_tag='v1.1.0'
stac_api_tag='v1.0.0'


function get() {
    echo "$1"
    wget -r "$1"
}

get 'https://geojson.org/schema/Geometry.json'
get 'https://geojson.org/schema/FeatureCollection.json'
get 'https://geojson.org/schema/Feature.json'
get https://proj.org/schemas/v0.7/projjson.schema.json


# strip the 'v' from the start if there.
stac_version="${stac_tag#v}"
stac_api_version="${stac_api_tag#v}"
subfolder="stac-spec-${stac_version}"

set -x

# Clean before updating.
rm -rf schemas.stacspec.org
mkdir schemas.stacspec.org

# GNU Tar Requires a --wildcards flag to use wildcards in the extraction include pattern
# BSD/Mac Tar does not
WILDCARD=$(tar --version 2>&1 | grep -q 'GNU' && echo "--wildcards" || echo "")

curl -L https://github.com/radiantearth/stac-spec/archive/${stac_tag}.tar.gz | \
  tar -xvf "${WILDCARD}" - "${subfolder}/*/json-schema/*.json"

mv "${subfolder}" "schemas.stacspec.org/${stac_tag}"
rm -f "stac/${stac_version}"
# The path to the linked folder needs to be relative to the path of the symlink.
ln -s "../schemas.stacspec.org/${stac_tag}" "stac/${stac_version}"

api_subfolder="stac-api-spec-${stac_api_version}"
curl -L https://github.com/radiantearth/stac-api-spec/archive/${stac_api_tag}.tar.gz | \
  tar -xvf "${WILDCARD}" - "${api_subfolder}/*/json-schema/*.json"

mv "${api_subfolder}/stac-spec" "schemas.stacspec.org/${stac_api_version}"
rm -f "stac-api/${stac_api_version}"
ln -s "../schemas.stacspec.org/${stac_api_version}" "stac-api/"

# The ItemCollection was removed from core stac, but is used by stac-api.
wget https://raw.githubusercontent.com/radiantearth/stac-spec/568a04821935cc92de7b4b05ea6fa9f6bf8a0592/item-spec/json-schema/itemcollection.json
perl -pi -e 's#"const": "0.9.0"#"const": "1.1.0"#g' itemcollection.json
mv itemcollection.json "stac/${stac_version}/item-spec/json-schema"

# Convert all the line endings to Unix style
# Some of the stac spec ones have CRLF :(
find . -type f -exec perl -i -pe 's/\r$//' {} \;

# As of 2025-09-19 fastjsonschema fails on a null set of checks in collection.json
# A fix to fastjsonschema will be submitted, but for now, just delete the useless section.
COLLECTION_JSON=schemas.stacspec.org/v1.1.0/collection-spec/json-schema/collection.json
jq '.definitions.summaries.additionalProperties.anyOf |= map(if .title == "Set of values" then del(.items) else . end)' "${COLLECTION_JSON}" > modified_collection.json
mv modified_collection.json "${COLLECTION_JSON}"

# The $id in schemas.stacspec.org/v1.1.0/item-spec/json-schema/common.json is missing a dot, it should be
# fixed upstream too.
perl -pi -e 's/commonjson/common.json/g' schemas.stacspec.org/v1.1.0/item-spec/json-schema/common.json

echo "Success"
echo "If git status shows any changes, rerun tests, and commit them"
