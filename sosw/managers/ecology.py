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
    DEFAULT_CONFIG = {
    }

    running_tasks = defaultdict(int)
    task_client = None  # Will be Circular import! Careful!


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def __call__(self, event):
        raise NotImplemented


    @property
    def eco_statuses(self):
        return [x[0] for x in ECO_STATUSES]


    def get_labourer_status(self, labourer: Labourer) -> int:
        """ FIXME """
        return random.choice(self.eco_statuses)


    def get_running_tasks_for_labourer(self, labourer: Labourer) -> int:

        logger.info(f"Called get_running_tasks_for_labourer for {labourer}")
        # Circular import! Careful!
        if not self.task_client:
            logger.info("Initialising TaskManager from EcologyManager. "
                        "This is circular import and it should point to already existing Class instance.")
            from .task import TaskManager
            self.task_client = TaskManager(custom_config={1:1})
            # self.register_clients(['Task'])

        if labourer.id not in self.running_tasks.keys():
            self.running_tasks[labourer.id] = self.task_client.get_running_tasks_for_labourer(labourer)

        logger.debug(f"EcologyManager.get_running_tasks_for_labourer() returns: {self.running_tasks[labourer.id]}")
        return self.running_tasks[labourer.id]


    def add_running_tasks_for_labourer(self, labourer: Labourer, count: int = 1) -> int:
        """
        Adds to the current counter of running tasks the given `count`.
        Invokes the getter first in case the original number was not yet calculated from DynamoDB.
        """

        self.running_tasks[labourer.id] = self.get_running_tasks_for_labourer(labourer) + count


    def get_labourer_average_duration(self, labourer: Labourer) -> int:
        return 300


    def get_labourer_max_duration(self, labourer: Labourer) -> int:
        return 300
