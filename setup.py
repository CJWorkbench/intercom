#!/usr/bin/env python

from setuptools import setup

setup(
    name="intercom",
    version="0.0.1",
    description="Download user lists from Intercom",
    author="Adam Hooper",
    author_email="adam@adamhooper.com",
    url="https://github.com/CJWorkbench/intercom",
    packages=[""],
    py_modules=["libraryofcongress"],
    install_requires=["pandas==0.25.0", "cjwmodule>=1.3.0"],
)
