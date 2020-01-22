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

__all__ = ['Scavenger']
__author__ = "Sophie Fogel, Nikolay Grishchenko"
__version__ = "1.0"

import logging
from typing import Dict

from sosw.essential import Essential
from sosw.labourer import Labourer
from sosw.managers.task import TaskManager


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Scavenger(Essential):
    """
    Scavenger main class performes the following operations:

    - archive_tasks(labourer)
    - handle_expired_tasks(labourer)
    - retry_tasks(labourer)
    """

    DEFAULT_CONFIG = {
        'init_clients':      ['Task', 'Sns'],
        'sns_config':        {
            'recipient': 'arn:aws:sns:us-west-2:000000000000:sosw_info',
            'subject':   '``sosw`` Info'
        },
        'retry_tasks_limit': 20  # TODO: What's the optimal number?
    }

    # these clients will be initialized by Processor constructor
    task_client: TaskManager = None
    sns_client = None


    def __call__(self, *args, **kwargs):
        logger.info(f"Called Scavenger.__call__ with args={args}, kwargs={kwargs}")

        labourers = self.task_client.register_labourers()

        for labourer in labourers:
            self.archive_tasks(labourer)
            self.handle_expired_tasks(labourer)
            self.retry_tasks(labourer)


    def handle_expired_tasks(self, labourer: Labourer):
        logger.debug(f"Called Scavenger.handle_expired_tasks with labourer={labourer}")
        expired_tasks = self.task_client.get_expired_tasks_for_labourer(labourer)
        logger.debug(f"expired_tasks: {expired_tasks}")
        for task in expired_tasks:
            self.process_expired_task(labourer, task)


    def process_expired_task(self, labourer: Labourer, task: Dict):
        logger.info(f"Called Scavenger.process_expired_task with labourer={labourer}, task={task}")
        _ = self.get_db_field_name

        if self.should_retry_task(labourer, task):
            self.move_task_to_retry_table(task, labourer)
        else:
            logger.info(f"Closing dead task {task}")
            self.sns_client.send_message(f"Closing dead task: {task[_('task_id')]} ", subject='``sosw`` Dead Task')
            self.task_client.archive_task(task[_('task_id')])
            self.stats['closed_dead_tasks'] += 1


    def should_retry_task(self, labourer: Labourer, task: Dict) -> bool:
        logger.debug(f"Called Scavenger.should_retry_task with labourer={labourer}, task={task}")
        attempts = task.get(self.get_db_field_name('attempts'))
        return True if attempts < labourer.get_attr('max_attempts') else False


    def move_task_to_retry_table(self, task: Dict, labourer: Labourer):
        """
        Put the task to a Dynamo table `sosw_retry_tasks`, with the wanted delay: labourer.max_runtime * attempts.
        Delete it from `sosw_tasks` table.
        """

        logger.debug(f"Called Scavenger.move_task_to_retry_table with labourer={labourer}, task={task}")
        wanted_delay = self.calculate_delay_for_task_retry(labourer, task)
        self.task_client.move_task_to_retry_table(task, wanted_delay)


    def calculate_delay_for_task_retry(self, labourer: Labourer, task: Dict) -> int:
        logger.debug(f"Called Scavenger.calculate_delay_for_task_retry with labourer={labourer}, task={task}")
        attempts = task[self.get_db_field_name('attempts')]
        wanted_delay = labourer.get_attr('max_duration') * attempts
        return wanted_delay


    def retry_tasks(self, labourer: Labourer):
        """
        Read from dynamo table `sosw_retry_tasks`, get tasks with retry_time <= now, and put them to `sosw_tasks` in the
        beginning of the queue.
        """

        logger.debug(f"Running Scavenger.retry_tasks")
        tasks_to_retry = self.task_client.get_tasks_to_retry_for_labourer(labourer=labourer,
                                                                          limit=self.config.get('retry_tasks_limit'))
        self.task_client.retry_tasks(labourer=labourer, tasks=tasks_to_retry)


    def archive_tasks(self, labourer: Labourer):
        """
        Read from `sosw_tasks` the ones successfully marked as completed by Workers and archive them.
        """

        _ = self.get_db_field_name

        logger.debug(f"Running Scavenger.archive_tasks for {labourer.id}")

        tasks = self.task_client.get_completed_tasks_for_labourer(labourer)

        for task in tasks:
            logger.info(f"Archiving completed_task: {task}")
            self.task_client.archive_task(task[_('task_id')])


    def get_db_field_name(self, key: str) -> str:
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.task_client.get_db_field_name(key)
