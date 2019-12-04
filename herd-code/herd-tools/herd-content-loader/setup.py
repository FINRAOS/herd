"""
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

"""
    herdcontentloader

    The herd data management module for populating content to UDC
"""

from setuptools import setup, find_packages

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools
setup(
    name="herdcl",
    version="0.110.1",
    description="herd data management",
    maintainer="FINRA",
    maintainer_email="herd@finra.org",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    url="https://github.com/FINRAOS/herd",
    keywords=["herd", "dm"],
    install_requires=[
        "herdsdk", "numpy >= 1.17.0", "pandas >= 0.24.0", "boto3 >= 1.9.50", "xlrd >= 1.1.0"
    ],
    packages=find_packages(),
    include_package_data=True,
    long_description="""\
    The herd data management module for populating content to UDC
    """
)
