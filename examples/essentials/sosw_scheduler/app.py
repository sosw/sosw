"""Sosw Essential Scheduler
"""

import logging

from sosw.scheduler import Scheduler
from sosw.app import LambdaGlobals, get_lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class EssentialScheduler(Scheduler):
    pass


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(EssentialScheduler, global_vars)
