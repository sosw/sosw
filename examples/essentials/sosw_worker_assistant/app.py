"""Sosw Essential WorkerAssistant
"""

import logging

from sosw.worker_assistant import WorkerAssistant
from sosw.app import LambdaGlobals, get_lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class EssentialWorkerAssistant(WorkerAssistant):
    pass


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(EssentialWorkerAssistant, global_vars)
