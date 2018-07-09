from sqlalchemy import case, cast, func, literal, select
from sqlalchemy.dialects import postgresql as postgres

from datacube import Datacube
from datacube.drivers.postgres._fields import NativeField
from datacube.drivers.postgres._schema import DATASET, METADATA_TYPE
from datacube.model import MetadataType


def get_dataset_extent_alchemy_expression(md: MetadataType):
    """
    Build an SQLaLchemy expression to get the extent for a dataset.

    It's returned as a postgis geometry.

    The logic here mirrors the extent() function of datacube.model.Dataset.
    """
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    projection_offset = md.definition["dataset"]["grid_spatial"]

    projection = doc[projection_offset]

    valid_data_offset = projection_offset + ["valid_data"]
    geo_ref_points_offset = projection_offset + ["geo_ref_points"]

    # If we have valid_data offset, return it as a polygon.
    return case(
        [
            (
                projection[valid_data_offset] != None,
                func.ST_GeomFromGeoJSON(doc[valid_data_offset].astext),
            )
        ],
        # Otherwise construct a polygon from the four corner points.
        else_=func.ST_MakePolygon(
            func.ST_MakeLine(
                postgres.array(
                    tuple(
                        _gis_point(doc[geo_ref_points_offset + [key]])
                        for key in ("ll", "ul", "ur", "lr", "ll")
                    )
                )
            )
        ),
    )


def get_dataset_crs_alchemy_expression(md: MetadataType):
    doc = md.dataset_fields["metadata_doc"].alchemy_expression

    projection_offset = md.definition["dataset"]["grid_spatial"]

    # Most have a spatial_reference field we can use directly.
    spatial_reference_offset = projection_offset + ["spatial_reference"]
    return func.coalesce(
        doc[spatial_reference_offset].astext,
        # Some older datasets have datum/zone fields instead.
        # The only remaining ones in DEA are 'GDA94'.
        case(
            [
                (
                    doc[(projection_offset + ["datum"])].astext == "GDA94",
                    "EPSG:283" + doc[(projection_offset + ["zone"])].astext,
                )
            ],
            else_=None,
        ),
    )


def _gis_point(obj):
    return func.ST_MakePoint(
        cast(obj["x"].astext, postgres.DOUBLE_PRECISION),
        cast(obj["y"].astext, postgres.DOUBLE_PRECISION),
    )


if __name__ == "__main__":
    with Datacube(env="clone") as dc:

        eo_type = dc.index.metadata_types.get_by_name("eo")
        for row in dc.index.datasets._db._engine.execute(
            select(
                [
                    DATASET.c.id,
                    get_dataset_extent_alchemy_expression(eo_type).label("geom"),
                    get_dataset_crs_alchemy_expression(eo_type).label("crs"),
                    eo_type.dataset_fields["time"].alchemy_expression.label("time"),
                ]
            )
            .select_from(DATASET.join(METADATA_TYPE))
            .where(METADATA_TYPE.c.id == eo_type.id)
            .limit(1)
        ).fetchall():
            print(repr(row))
