"""
    herd-pandas

    The herd data management module for use with python pandas and jupyter notebooks.
"""

from setuptools import setup, find_packages

VERSION = "0.89.0-SNAPSHOT.20190130212515"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools
setup(
    name="herd-pandas",
    version=VERSION,
    description="herd data management",
    author_email="",
    url="",
    keywords=["herd", "dm", "pandas"],
    install_requires=["herd_sdk", "numpy >= 1.16.0", "pandas >= 0.24.0"],
    packages=find_packages(),
    include_package_data=True,
    long_description="""\
    The herd data management module for use with python pandas and jupyter notebooks.
    """
)
