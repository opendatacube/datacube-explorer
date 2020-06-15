"""
Test utility method for creating valid EPSG code based CRS from
possible WKT String
"""
from cubedash._utils import infer_crs

TEST_CRS_RAW = """
GEOGCS["GEOCENTRIC DATUM of AUSTRALIA",DATUM["GDA94",SPHEROID["GRS80",6378137,298.257222101]],
PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]
"""


def test_crs_infer_fail():
    assert infer_crs("") is None


def test_crs_infer_pass():
    assert infer_crs(TEST_CRS_RAW) == "epsg:4283"
