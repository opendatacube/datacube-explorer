#!/usr/bin/env python3
import pathlib

from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent.resolve()

README = (HERE / "README.md").read_text()

tests_require = [
    "black",
    "docutils",
    "boltons",
    "deepdiff",
    "digitalearthau",
    "flake8",
    "jsonschema > 3",
    "pre-commit",
    "pytest",
    "pytest-benchmark",
    "pytest-cov",
    "requests-html",
    "raven",
    "blinker",
    "prometheus-flask-exporter",
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
        "gevent",
        # Monitoring
        "raven",
        "blinker",
        "prometheus-flask-exporter",
    ],
}

extras_require["test"].extend(extras_require["deployment"])

setup(
    name="datacube-explorer",
    description="Web-based exploration of Open Data Cube collections",
    long_description=README,
    long_description_content_type="text/markdown",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    python_requires=">=3.7",
    url="https://github.com/opendatacube/datacube-explorer",
    author="Geoscience Australia",
    author_email="earth.observation@ga.gov.au",
    license="Apache Software License 2.0",
    packages=find_packages(exclude=("integration_tests",)),
    project_urls={
        "Bug Reports": "https://github.com/opendatacube/datacube-explorer/issues",
        "Source": "https://github.com/opendatacube/datacube-explorer",
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    install_requires=[
        "cachetools",
        "click",
        "datacube>=1.8",
        "eodatasets3>=0.23.1",
        "fiona",
        "flask",
        "Flask-Caching",
        "flask-cors",
        "flask-themer>=1.4.3",
        "geoalchemy2",
        "geographiclib",
        "jinja2",
        "markupsafe",
        "pyorbital",
        "pyproj",
        "python-dateutil",
        "orjson>=3",
        "shapely",
        "simplekml",
        "sqlalchemy>=1.3",
        "structlog>=20.2.0",
    ],
    tests_require=tests_require,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "cubedash-gen = cubedash.generate:cli",
            "cubedash-view = cubedash.summary.show:cli",
            "cubedash-run = cubedash.run:cli",
            "cubedash-page-test = cubedash.warmup:cli",
        ]
    },
)
