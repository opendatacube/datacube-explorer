#!/usr/bin/env python3

from setuptools import find_packages, setup

tests_require = ["pylint", "digitalearthau"]

dependency_links = [
    "git+git://github.com/GeoscienceAustralia/digitalearthau@develop#egg=digitalearthau"
]

setup(
    name="dea-dashboard",
    url="https://github.com/data-cube/dea-dashboard",
    author="Geoscience Australia",
    packages=find_packages(),
    install_requires=[
        "cachetools",
        "datacube",
        "flask",
        "Flask-Caching",
        "gunicorn",
        "jinja2",
        "meinheld",
        "python-dateutil",
        "shapely",
    ],
    tests_require=tests_require,
    dependency_links=dependency_links,
)
