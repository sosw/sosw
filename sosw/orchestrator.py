import boto3
import logging
import os

from importlib import import_module
from collections import defaultdict

from sosw.app import Processor

__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"


__all__ = ['Orchestrator']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Orchestrator(Processor):
    """
    Orchestrator class.
    """

    DEFAULT_CONFIG = {}
