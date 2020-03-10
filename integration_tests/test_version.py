"""Test this cubedash build has a valid version
"""
import cubedash


def test_check_version():
    assert cubedash.__version__ != "Not-Installed"
