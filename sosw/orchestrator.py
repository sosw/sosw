__all__ = ['Orchestrator']

__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

import boto3
import logging
import math
import os

from importlib import import_module
from collections import defaultdict
from typing import List

from sosw.app import Processor
from sosw.labourer import Labourer
from sosw.managers.task import TaskManager


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Orchestrator(Processor):
    """
    Orchestrator class.
    """

    DEFAULT_CONFIG = {
        'init_clients':                     ['Task', 'Ecology'],
        'invocation_number_coefficient':    {
            0: 0,
            1: 0,
            2: 0.5,
            3: 0.75,
            4: 1
        },
        'labourers':                        {
            # 'some_function': {
            #     'arn': 'arn:aws:lambda:us-west-2:0000000000:function:some_function',
            #     'max_simultaneous_invocations': 10,
            # }
        },
        'default_simultaneous_invocations': 2
    }


    def __call__(self, event):
        labourers = self.get_labourers()
        for labourer in labourers:
            self.invoke_for_labourer(labourer)


    def invoke_for_labourer(self, labourer: Labourer):
        number_of_tasks = self.get_desired_invocation_number_for_labourer(labourer=labourer)

        tasks_to_process = self.task_client.get_next_for_labourer(worker=labourer, cnt=number_of_tasks)
        logger.info(tasks_to_process)

        for task in tasks_to_process:
            self.task_client.invoke_task(task_id=task, labourer=labourer)


    def get_labourer_setting(self, labourer: Labourer, attribute: str):
        """ Should probably try to use some default values, but for now we delegate this to whoever calls me. """

        try:
            return self.config['labourers'][labourer.id][attribute]
        except KeyError:
            logger.info(f"CONFIG WAS: {self.config} and did not find: {attribute} for {labourer.id}")
            return None


    def get_desired_invocation_number_for_labourer(self, labourer: Labourer) -> int:
        """
        Decides the desired maximum number of simultaneous invocations for a specific Labourer.
        The decision is based on the ecology status of the Labourer and the configs.

        :param labourer: Labourer type.
        :return: Number of invocations
        """

        labourer_status = self.ecology_client.get_labourer_status(labourer=labourer)

        coefficient = next(v for k, v in self.config['invocation_number_coefficient'].items() if labourer_status == k)

        max_invocations = self.get_labourer_setting(labourer, 'max_simultaneous_invocations') \
                          or self.config['default_simultaneous_invocations']

        return math.floor(max_invocations * coefficient)


    def get_labourers(self):
        """
        Return configured Labourers as a dict with 'name' as key.
        Config of the Orchestrator expects 'labourers' as a dict 'name_of_lambda': {'some_setting': 'value1'}
        """

        return {name: Labourer(id=name, **settings) for name, settings in self.config['labourers'].items()}
