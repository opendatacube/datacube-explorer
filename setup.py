#!/usr/bin/env python3

from setuptools import setup, find_packages

import versioneer

tests_require = [
    'pylint',
    'digitalearthau',
]

dependency_links = [
    'git+git://github.com/GeoscienceAustralia/digitalearthau/release/latest#egg=digitalearthau'
]

setup(
    name='dea-dashboard',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),

    url='https://github.com/data-cube/dea-dashboard',
    author='Geoscience Australia',
    
    packages=find_packages(),
    install_requires=[
        'datacube',
        'cachetools',
        'flask',
        'jinja2',
        'python-dateutil',
        'shapely'
    ],
    tests_require=tests_require,
    dependency_links=dependency_links,
)
    

