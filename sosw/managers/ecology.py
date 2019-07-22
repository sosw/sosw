"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers
    Copyright (C) 2019  sosw core contributors

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/gpl-3.0.html>.
"""

__all__ = ['EcologyManager', 'ECO_STATUSES']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import boto3
import datetime
import json
import logging
import operator
import os
import random
import time

from collections import defaultdict
from collections import OrderedDict
from statistics import mean
from typing import Dict, List, Optional, Union

from sosw.app import Processor
from sosw.labourer import Labourer
from sosw.components.benchmark import benchmark
from sosw.components.helpers import make_hash
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
        'init_clients': ['cloudwatch'],
        'default_metric_values':
                        {
                            'Period':                     60,
                            'Statistics':                 ['Average'],
                            'MetricAggregationTimeSlice': 300
                        }
    }

    running_tasks = defaultdict(int)
    health_metrics: Dict = None
    task_client: TaskManager = None  # Will be Circular import! Careful!
    cloudwatch_client: boto3.client = None


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def __call__(self, event):
        raise NotImplementedError


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

        logger.info("Reset cache of health_metrics in EcologyManager")
        self.health_metrics = dict()


    @property
    def eco_statuses(self):
        return [x[0] for x in ECO_STATUSES]


    def fetch_metric_stats(self, metric: Dict) -> List[Dict]:
        """
        Fetches from CloudWatch Datapoints of aggregated metric statistics.
        Fields in `metric` are the attributes of get_metric_statistics_.
        Additional parameter: MetricAggregationTimeSlice in seconds is used to calculate the Start and EndTime.

        .. _get_metric_statistics: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.get_metric_statistics

        If some fields are missing in the metric, the defaults come from ``config['default_metric_values']``
        """

        _cfg = self.config.get

        COMPARATORS = {
            'Average': mean,
            'Maximum': max,
            'Minimum': min,
        }

        params = metric.copy()

        # Setting up default metric parameters if unspecified for current metric.
        for k, v in _cfg('default_metric_values', {}).items():
            if k not in metric:
                params[k] = v
                logger.debug(f"Set the default {v} for {k} in metric query {metric}.")

        assert len(params['Statistics']) == 1, "Complex statistics aggregation is not yet supported"

        duration = int(params.pop('MetricAggregationTimeSlice'))

        params['EndTime'] = datetime.datetime.now()
        params['StartTime'] = params['EndTime'] - datetime.timedelta(seconds=duration)

        logger.debug(f"Query to CloudWatch `get_metric_statistics`: {params}")
        result = self.cloudwatch_client.get_metric_statistics(**params)

        comparator_name = params['Statistics'][0]
        comparator = COMPARATORS[comparator_name]

        return comparator(x[comparator_name] for x in result.get('Datapoints', list()))


    def get_labourer_status(self, labourer: Labourer) -> int:
        """
        Get the worst (lowest) health status according to preconfigured health metrics of the Labourer.

        .. _ECO_STATUSES:

        Current ECO_STATUSES:

        - (0, 'Bad')
        - (1, 'Poor')
        - (2, 'Moderate')
        - (3, 'Good')
        - (4, 'High')
        """

        _cfg = self.config.get

        health = max(map(lambda x: int(x[0]), ECO_STATUSES))

        metrics = getattr(labourer, 'health_metrics', {}) or {}
        for health_metric in metrics.values():

            metric_hash = make_hash(health_metric['details'])
            if metric_hash not in self.health_metrics:
                self.health_metrics[metric_hash] = self.fetch_metric_stats(metric=health_metric['details'])
                logger.info(f"Updated the cache of Ecology metric {metric_hash} - {health_metric} "
                            f"with {self.health_metrics[metric_hash]}")

            value = self.health_metrics[metric_hash]
            logger.debug(f"Ecology metric {metric_hash} has {value}")

            health = min(health, self.get_health(value, metric=health_metric))

        logger.info(f"Ecology health of Labourer {labourer} is {health}")

        return health


    def get_health(self, value: Union[int, float], metric: Dict) -> int:
        """
        Checks the value against the health_metric configuration.
        """

        op = getattr(operator, metric.get('feeling_comparison_operator'))

        # Find the first configured feeling from the map that does not comply.
        # Order and validate the feelings
        feelings = OrderedDict([(key, metric['feelings'][key])
                                for key in sorted(metric['feelings'].keys(), reverse=True)])

        last_target = 0
        for health, target in feelings.items():
            if op(target, last_target):
                raise ValueError(f"Order of values if feelings is invalid and doesn't match expected eco statuses: "
                                 f"{feelings.items()}. Failed: {last_target} not "
                                 f"{metric.get('feeling_comparison_operator')} {target}")

            if op(value, target):
                return int(health)

            last_target = target

        return 0


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
            logger.debug(f"EcologyManager.count_running_tasks_for_labourer() recalculated cache for Labourer "
                         f"{labourer}")

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
        """

        resp = self.task_client.lambda_client.get_function_configuration(FunctionName=labourer.arn)
        return resp['Timeout']


    # The task_client of EcologyManager is just a pointer. We skip recursive stats to avoid infinite loop.
    def get_stats(self, recursive=False):
        return super().get_stats(recursive=False)


    def reset_stats(self, recursive=False):
        return super().reset_stats(recursive=False)
