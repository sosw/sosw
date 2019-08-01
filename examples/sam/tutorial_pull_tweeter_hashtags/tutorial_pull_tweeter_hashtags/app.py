"""tutorial_pull_tweeter_hashtags
"""

import logging

from sosw.worker import Worker
from sosw.app import LambdaGlobals, get_lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Processor(Worker):
    pass


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
