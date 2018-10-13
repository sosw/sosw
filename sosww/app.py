import logging
import os

from collections import defaultdict

from .components.helpers import *
from .components.ssm import get_config
from .components.tasks_api_client_for_workers import close_task


__author__ = "Nikolay Grishchenko"
__version__ = "1.00"

__all__ = ['Processor']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Processor:
    """
    Worker class template.
    """

    DEFAULT_CONFIG = {}


    def __init__(self, custom_config=None, **kwargs):
        """
        Initialize the Processor.
        Updates the default config with parameters from SSM, then from provided custom config (usually from event).
        """

        self.test = kwargs.get('test') or True if os.environ.get('STAGE') in ['test', 'autotest'] else False

        if self.test and not custom_config:
            raise RuntimeError("You must specify a custom config from your testcase to run processor in test mode.")

        self.config = self.DEFAULT_CONFIG.copy()
        self.config.update(get_config('YOUR_FUNCTION_NAME_config'))
        self.config.update(custom_config or {})
        logger.info(f"Final processor config: {self.config}")

        self.stats = defaultdict(int)


    def __call__(self, event):
        """
        Call the Processor
        """

        logger.info("Called the Processor")
        raise NotImplementedError("The main worker function in Processor is missing")

        # Mark the task as completed in DynamoDB.
        close_task(event)


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Destructor.
        """
        pass


    def get_stats(self):
        """
        Return statistics of operations performed by current instance of the Class.

        :return:    key: int statistics.
        """
        return self.stats
