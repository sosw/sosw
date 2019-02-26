__all__ = ['EcologyManager', 'ECO_STATUSES']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import boto3
import json
import logging
import os
import random
import time

from collections import defaultdict
from typing import Dict, List, Optional

from sosw.components.benchmark import benchmark
from sosw.app import Processor


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ECO_STATUSES = (
    (0, 'Bad'),
    (1, 'Poor'),
    (2, 'Moderate'),
    (3, 'Good'),
    (4, 'High'),
)


class EcologyManager(Processor):
    DEFAULT_CONFIG = {}


    def __call__(self, event):
        raise NotImplemented


    @property
    def eco_statuses(self):
        return [x[0] for x in ECO_STATUSES]


    def get_worker_status(self, worker_id: int) -> int:
        return random.choice(self.eco_statuses)
