"""
Test utility method for creating valid EPSG code based CRS from
possible WKT String
"""
from pyproj import CRS

from cubedash._utils import infer_crs

# This CRS was embedded in DEA's gamma-ray product definition.
TEST_CRS_RAW = """
GEOGCS["GEOCENTRIC DATUM of AUSTRALIA",DATUM["GDA94",SPHEROID["GRS80",6378137,298.257222101]],
PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]
"""


def test_crs_infer_has_match_floor():
    """Don't match something that's too different"""
    assert infer_crs("") is None


def test_crs_infer_pass():
    assert infer_crs(TEST_CRS_RAW) == "epsg:4283"


def test_crs_infers_itself():
    """Sanity check: something should match itself!"""
    assert infer_crs(CRS.from_epsg(4326).to_wkt()) == "epsg:4326"
