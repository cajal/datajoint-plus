#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# read in version number into __version__
with open(path.join(here, 'datajoint_plus', 'version.py')) as f:
    exec(f.read())

setup(
    name='datajoint-plus',
    version=__version__,
    description="A DataJoint extension that integrates hashes and other features.",
    author='Stelios Papadopoulos',
    author_email='spapadop@bcm.edu',
    license="GNU LGPL",
    packages=find_packages(),
    install_requires=['datajoint==0.12.9'],
    python_requires='~=3.6'
)