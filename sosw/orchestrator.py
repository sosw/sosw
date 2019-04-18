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
        'init_clients':                     ['Task'],
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
    # ecology_client: EcologyManager = None


    def __call__(self, event):

        labourers = self.task_client.register_labourers()

        for labourer in labourers:
            self.invoke_for_labourer(labourer)


    def invoke_for_labourer(self, labourer: Labourer):
        """
        Invokes required queued tasks for `labourer`.
        """

        number_of_tasks = self.get_desired_invocation_number_for_labourer(labourer=labourer)

        if number_of_tasks < 1:
            logger.info(f"Should not invoke any tasks for Labourer: {labourer.id}")
            return

        tasks_to_process = self.task_client.get_next_for_labourer(labourer=labourer, cnt=number_of_tasks)

        if tasks_to_process:
            logger.info(f"Decided to invoke the following tasks for {labourer.id}: {tasks_to_process}")

            for task in tasks_to_process:
                self.task_client.invoke_task(task=task, labourer=labourer)


    def get_desired_invocation_number_for_labourer(self, labourer: Labourer) -> int:
        """
        Decides the desired maximum number of simultaneous invocations for a specific Labourer.
        The decision is based on the ecology status of the Labourer and the configs.

        :return: Number of invocations
        """

        labourer_status = self.task_client.ecology_client.get_labourer_status(labourer=labourer)

        coefficient = next(v for k, v in self.config['invocation_number_coefficient'].items() if labourer_status == k)

        labourer_max = labourer.get_attr('max_simultaneous_invocations')

        max_invocations = labourer_max if labourer_max is not None else self.config['max_simultaneous_invocations']

        desired = int(math.floor(max_invocations * coefficient))
        currently_running = self.task_client.ecology_client.count_running_tasks_for_labourer(labourer)

        logger.info(f"Labourer: {labourer.id} has currently running {currently_running} tasks and desired {desired} "
                    f"with respect to status {labourer_status}.")
        return max(desired - currently_running, 0)


    def get_labourers(self) -> List[Labourer]:
        """
        Gets a list of pre-configured Labourers from TaskManager.

        :return:
        """
        return self.task_client.get_labourers()
