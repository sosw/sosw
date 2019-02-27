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

from sosw.app import Processor
from sosw.labourer import Labourer
from sosw.components.benchmark import benchmark


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


    def get_labourer_status(self, labourer: Labourer) -> int:
        return random.choice(self.eco_statuses)


    def get_running_tasks_for_labourer(self, labourer: Labourer):
        pass
