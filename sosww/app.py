import boto3
import datetime
import logging
import time

from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


__all__ = ['Processor']


class Processor:
    """
    The core class of `sosw` package.
    """

    def __init__(self, custom_config=None):
        self.stats = defaultdict(int)


    def __call__(self, event=None):
        pass


    def get_stats(self):
        """
        Return statistics of operations performed by current instance of the Class.

        :return:    {key: int} statistics.
        """

        return self.stats


def lambda_handler(event, context):
    """
    Entry point of the Lambda invocation.
    """

    processor = Processor(custom_config=event.get('config'))
    processor()

    result = processor.get_stats()

    logger.info(result)
    return result
