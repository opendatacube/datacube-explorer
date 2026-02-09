"""
Define custom transform_safe sql function to filter out geometries that can't be
transformed to 4326. This is intended as a temporary workaround to avoid having
having a handful of products crash the entire stac/collections endpoint until a
more permanent solution is implemented.
"""

from geoalchemy2 import Geometry
from sqlalchemy.sql.functions import GenericFunction

from cubedash.summary._schema import CUBEDASH_SCHEMA


class SafeTransform(GenericFunction):
    type = Geometry()
    package = CUBEDASH_SCHEMA
    identifier = "transform_safe"
    inherit_cache = False
    name = "transform_safe"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.packagenames = (CUBEDASH_SCHEMA,)


TRANSFORM_SAFE_SQL = f"""
    CREATE OR REPLACE FUNCTION {CUBEDASH_SCHEMA}.transform_safe(geom geometry, srid int)
    RETURNS geometry AS $$
    BEGIN
        RETURN ST_Transform(geom, srid);
    EXCEPTION WHEN internal_error THEN
        RETURN NULL;
    END;
    $$
    language plpgsql;
    """
