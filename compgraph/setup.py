#!/usr/bin/env python

from setuptools import setup

packages = {
    "graph": "lib",
    "graph.src": "lib/src"
}

setup(
    name='graph',
    version='1.0',
    description='',
    author='Pavlovskaya Anastaiia',
    author_email='anastasiya.pavlovskaya@phystech.edu',
    install_requires=[],
    packages=packages,
    package_dir=packages
)
