#!/usr/bin/env python

from distutils.core import setup

setup(
    name='polygon_wrapper',
    version='0.1',
    description='Polygon.io flat file downloader',
    py_modules=["polygon_wrapper", "cli"],
    entry_points={
        "console_scripts": {
            "polygon_download=cli:main"
        }
    },
    install_requires=[
        'boto3',
        'polars',
        'pandas',
        'dateparser'

    ]
)
