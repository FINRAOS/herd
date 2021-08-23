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
# Standard library imports
import logging
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
import sys


def get_logger(name):
    """
    Get logger to output to file and stdout

    :param name: name of module calling method
    :return: the logger
    """
    log_handler = RotatingFileHandler('debug.log', mode='a', maxBytes=250 * 1024, backupCount=1)
    stream_handler = StreamHandler(sys.stdout)

    logging.basicConfig(format="%(asctime)s - %(module)s - Line %(lineno)d - %(levelname)s \n%(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[log_handler, stream_handler])

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def get_level(debug=False):
    """
    Get logging level

    :param debug: name of module calling method
    :type debug: bool
    :return: the logger
    """
    if debug:
        return logging.DEBUG
    else:
        return logging.INFO
