#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# read in version number into __version__
with open(path.join(here, 'datajoint_plus', 'version.py')) as f:
    exec(f.read())

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().split()

setup(
    name='datajoint-plus',
    version=__version__,
    description="A DataJoint extension that integrates hashes and other features.",
    author='Stelios Papadopoulos',
    author_email='spapadop@bcm.edu',
    license="GNU LGPL",
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['config/logging/templates/*.yml']},
    install_requires=requirements,
    python_requires='~=3.6'
)