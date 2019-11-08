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
import logging, logging.handlers
import sys


def get_logger(name):
    """
    Get logger to output to file and stdout

    :param name: name of module calling method
    :return: the logger
    """

    log_format = logging.Formatter("%(asctime)s - %(module)s - Line %(lineno)d - %(levelname)s \n%(message)s",
                                   "%Y-%m-%d %H:%M:%S")

    log_handler = logging.handlers.RotatingFileHandler('debug.log', mode='a', maxBytes=1024 * 1024)
    log_handler.setFormatter(log_format)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_format)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)
    logger.addHandler(stream_handler)
    return logger
