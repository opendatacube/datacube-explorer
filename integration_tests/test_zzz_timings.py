"""
Tests that use the app monitoring.

The timing decorator modifies global state, run these tests last.
"""

import pytest
from flask.testing import FlaskClient

from cubedash import _monitoring


# this test fails in gh with the postgis driver for unknown reasons
@pytest.mark.parametrize("env_name", ("default",), indirect=True)
def test_with_timings(client: FlaskClient) -> None:
    _monitoring.init_app_monitoring(client.application)
    # ga_ls8c_ard_3 dataset
    rv = client.get("/dataset/e2dd2539-ae18-4edc-a0e6-ddd31848669c")
    assert "Server-Timing" in rv.headers

    count_header = [
        f
        for f in rv.headers["Server-Timing"].split(",")
        if f.startswith("odcquerycount_")
    ]
    assert count_header, (
        f"No query count server timing header found in {rv.headers['Server-Timing']}"
    )

    # Example header:
    # app;dur=1034.12,odcquery;dur=103.03;desc="ODC query time",odcquerycount_6; \
    #    desc="6 ODC queries"
    _, val = count_header[0].split(";")[0].split("_")
    assert int(val) > 0, "At least one query was run, presumably?"
