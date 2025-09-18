import json
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

import fastjsonschema
from typing_extensions import Any

SCHEMAS = Path("/Users/aye011/dev/odc/datacube-explorer/integration_tests/schemas/")


def make_handler(base_path):
    def file_handler(filename_or_url: str) -> Any:
        print(f"loading schema from {filename_or_url}")
        o = urlparse(filename_or_url)

        path = o.path.lstrip("/")
        if o.netloc:
            absolute_path = base_path / o.netloc / path
        else:
            absolute_path = base_path / path

        if not absolute_path.exists():
            raise FileNotFoundError(
                f"Loading schema {filename_or_url} failed. Schema file not found: {absolute_path}"
            )
        with open(absolute_path) as f:
            return json.load(f)

    return file_handler


def load_schema(
    schema_path: Path | str, base_path: Path
) -> Callable[[dict[str, Any]], None]:
    """
    Create a JSON Schema validator from File or URL, but using only locally available schemas



    :param schema_path:
    :param base_path:
    :return:
    """
    absolute_schemas = make_handler(base_path.absolute())

    if isinstance(schema_path, str):  # and is_url(schema_path):
        o = urlparse(schema_path)
        path = o.path.lstrip("/")
        schema_path = base_path / o.netloc / path

    with open(schema_path) as schema_file:
        schema = json.load(schema_file)

    item_relative_schemas = make_handler(schema_path.absolute().parent)

    return fastjsonschema.compile(
        schema,
        handlers={
            "": item_relative_schemas,
            "file": absolute_schemas,
            "http": absolute_schemas,
            "https": absolute_schemas,
        },
    )


# validator = load_schema('/Users/aye011/dev/odc/datacube-explorer/integration_tests/schemas/schemas.stacspec.org/v1.1.0/collection-spec/json-schema/collection.json')
# validator = load_schema(Path('/Users/aye011/dev/odc/datacube-explorer/integration_tests/schemas/schemas.stacspec.org/v1.1.0/item-spec/json-schema/itemcollection.json'),
#                         base_path=SCHEMAS)
#
# validator({})
