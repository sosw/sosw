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
from sosw.managers.task import TaskManager


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
    task_client: TaskManager = None  # Will be Circular import! Careful!


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def __call__(self, event):
        raise NotImplemented


    def register_task_manager(self, task_manager: TaskManager):
        """
        We will have to make some queries, and don't want to initialise another TaskManager locally.
        Just receive the pointer to TaskManager from whoever needs.

        This could be in __init__, but I don't want to update the initialization workflows for every function
        initialising me. They usually use built-in in core Processor mechanism to register_clients().
        """

        logger.info("Registering TaskManager for EcologyManager")
        self.task_client = task_manager

        logger.info("Reset cache of running_tasks counter in EcologyManager")
        self.running_tasks = defaultdict(int)


    @property
    def eco_statuses(self):
        return [x[0] for x in ECO_STATUSES]


    def get_labourer_status(self, labourer: Labourer) -> int:
        """ FIXME """
        return 4
        # return random.choice(self.eco_statuses)


    def count_running_tasks_for_labourer(self, labourer: Labourer) -> int:
        """
        TODO Refactor this to cache the value in the Labourer object itself.
        Should also update add_running_tasks_for_labourer() for that.
        """

        if not self.task_client:
            raise RuntimeError("EcologyManager doesn't have a TaskManager registered. "
                               "You have to call register_task_manager() after initiazation and pass the pointer "
                               "to your TaskManager instance.")

        if labourer.id not in self.running_tasks.keys():
            self.running_tasks[labourer.id] = self.task_client.get_count_of_running_tasks_for_labourer(labourer)
            logger.debug(f"EcologyManager.count_running_tasks_for_labourer() recalculated cache for Labourer {labourer}")

        logger.debug(f"EcologyManager.count_running_tasks_for_labourer() returns: {self.running_tasks[labourer.id]}")
        return self.running_tasks[labourer.id]


    def add_running_tasks_for_labourer(self, labourer: Labourer, count: int = 1):
        """
        Adds to the current counter of running tasks the given `count`.
        Invokes the getter first in case the original number was not yet calculated from DynamoDB.
        """

        self.running_tasks[labourer.id] = self.count_running_tasks_for_labourer(labourer) + count


    def get_labourer_average_duration(self, labourer: Labourer) -> int:
        """
        Calculates the average duration of `labourer` executions.

        The operation consumes DynamoDB RCU . Normally this method is called for each labourer only once during
        registration of Labourers. If you want to learn this value, you should ask Labourer object.

        .. code-block::python

           some_labourer.get_attr('average_duration')
        """

        if not self.task_client:
            raise RuntimeError("EcologyManager doesn't have a TaskManager registered. "
                               "You have to call register_task_manager() after initiazation and pass the pointer "
                               "to your TaskManager instance.")

        return self.task_client.get_average_labourer_duration(labourer)


    def get_max_labourer_duration(self, labourer: Labourer) -> int:
        """
        Maximum duration of `labourer` executions.
        Should ask this from aws:lambda API, but at the moment use the hardcoded maximum.
        # TODO implement me.
        """

        return 900


    # The task_client of EcologyManager is just a pointer. We skip recursive stats to avoid infinite loop.
    def get_stats(self, recursive=False):
        return super().get_stats(recursive=False)


    def reset_stats(self, recursive=False):
        return super().reset_stats(recursive=False)
