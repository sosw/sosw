"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2019  sosw core contributors <info@sosw.app>

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

__all__ = ['Orchestrator']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import logging
import math

from typing import List

from sosw.essential import Essential
from sosw.labourer import Labourer
from sosw.managers.ecology import EcologyManager
from sosw.managers.task import TaskManager


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Orchestrator(Essential):
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
