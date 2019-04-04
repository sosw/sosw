__all__ = ['Orchestrator']

import logging
import math

from typing import List

from sosw.app import Processor
from sosw.labourer import Labourer
from sosw.managers.ecology import EcologyManager
from sosw.managers.task import TaskManager


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Orchestrator(Processor):
    """
    | Orchestrator class.
    | Iterates the pre-configured Labourers and invokes appropriate number of Tasks for each one.
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
        'default_simultaneous_invocations': 2
    }

    task_client: TaskManager = None
    ecology_client: EcologyManager = None


    def __call__(self, event):

        labourers = self.task_client.register_labourers()

        for labourer in labourers:
            self.invoke_for_labourer(labourer)


    def invoke_for_labourer(self, labourer: Labourer):
        """
        Invokes required queued tasks for `labourer`.
        """

        number_of_tasks = self.get_desired_invocation_number_for_labourer(labourer=labourer)

        tasks_to_process = self.task_client.get_next_for_labourer(labourer=labourer, cnt=number_of_tasks)
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

        :return: Number of invocations
        """

        labourer_status = self.ecology_client.get_labourer_status(labourer=labourer)

        coefficient = next(v for k, v in self.config['invocation_number_coefficient'].items() if labourer_status == k)

        max_invocations = self.get_labourer_setting(labourer, 'max_simultaneous_invocations') \
                          or self.config['default_simultaneous_invocations']

        return math.floor(max_invocations * coefficient)


    def get_labourers(self) -> List[Labourer]:
        """
        Gets a list of pre-configured Labourers from TaskManager.

        :return:
        """
        return self.task_client.get_labourers()
