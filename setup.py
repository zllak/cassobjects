#!/usr/bin/env python

"""Setup file for cassobjects"""

from setuptools import setup, find_packages

version_tuple = (0, 0, 1)
__version__ = '.'.join(map(str, version_tuple))

setup(
    name='cassobjects',
    version=__version__,
    description=open('README.rst', 'r').read(),
    author='Thomas Meson',
    author_email='zllak@hycik.org',
    maintainer='Thomas Meson',
    maintainer_email='zllak@hycik.org',
    url='https://github.com/zllak/cassobjects',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    requires=[
        "pycassa >= 1.2",
    ],
)
