import boto3
import logging
import os

from importlib import import_module
from collections import defaultdict

from sosww.components.helpers import *
from sosww.components.ssm import get_config
# from sosww.components.tasks_api_client_for_workers import close_task


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"


__all__ = ['Orchestrator']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Orchestrator:
    """
    Orchestrator class.
    """

    DEFAULT_CONFIG = {}

    def