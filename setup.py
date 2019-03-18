#!/usr/bin/env python3

from setuptools import find_packages, setup

import versioneer

tests_require = [
    "black",
    "boltons",
    "digitalearthau",
    "flake8",
    "isort[requirements]",
    "jsonschema > 3",
    "pytest",
    "pytest-cov",
    "requests-html",
]

extras_require = {
    "test": tests_require,
    # These are all optional but nice to have on a real deployment
    "deployment": [
        # Performance
        "ciso8601",
        "bottleneck",
        # The default run.sh and docs use gunicorn+meinheld
        "gunicorn",
        "setproctitle",
        "meinheld",
    ],
}

setup(
    name="dea-dashboard",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    python_requires=">=3.6",
    url="https://github.com/opendatacube/datacube-explorer",
    author="Geoscience Australia",
    author_email="earth.observation@ga.gov.au",
    packages=find_packages(),
    install_requires=[
        "cachetools",
        "click",
        "dataclasses>=0.6;python_version<'3.7'",
        "datacube>=1.6",
        "fiona",
        "flask",
        "Flask-Caching",
        "flask_themes @ git+https://git@github.com/maxcountryman/flask-themes@master",
        "geoalchemy2",
        "geographiclib",
        "jinja2",
        "pyorbital",
        "python-dateutil",
        "python-rapidjson",
        "shapely",
        "simplekml",
        "structlog",
    ],
    tests_require=tests_require,
    extras_require=extras_require,
    entry_points={"console_scripts": ["cubedash-gen = cubedash.generate:cli"]},
)
