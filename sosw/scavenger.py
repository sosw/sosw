__all__ = ['Scavenger']

__author__ = "Sophie Fogel"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

import logging
import time
from typing import Dict

from sosw.app import Processor
from sosw.labourer import Labourer


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Scavenger(Processor):
    DEFAULT_CONFIG = {
        'init_clients': ['Task', 'Ecology', 'Sns'],
        'dynamo_db_config': {
            'row_mapper':       {
                'task_id':     'S',
                'labourer_id': 'S',
                'greenfield':  'N',
                'attempts':    'N',
            },
            'required_fields':  ['task_id', 'labourer_id'],
            'table_name':       'sosw_tasks',
            'index_greenfield': 'sosw_tasks_greenfield',
            'field_names':      {
                'task_id':     'task_id',
                'labourer_id': 'labourer_id',
                'greenfield':  'greenfield',
            }
        },
        'sns_config': {
            'recipient': 'arn:aws:sns:us-west-2:0000000000:sosw_info',
            'subject':   'SOSW Info'
        },
        'retry_tasks_limit': 20  # TODO: What's the optimal number?
    }

    # these clients will be initialized by Processor constructor
    task_client = None
    ecology_client = None
    sns_client = None


    def __call__(self, *args, **kwargs):
        labourers = self.task_client.register_labourers()

        for labourer in labourers:
            self.handle_expired_tasks_for_labourer(labourer)

        self.retry_tasks()


    def handle_expired_tasks_for_labourer(self, labourer: Labourer):
        expired_tasks = self.task_client.get_expired_tasks_for_labourer(labourer)
        for task in expired_tasks:
            self.process_expired_task(labourer, task)


    def process_expired_task(self, labourer: Labourer, task: Dict):
        _ = self.get_db_field_name

        if self.should_retry_task(labourer, task):
            self.move_task_to_retry_table(task, labourer)
        else:
            self.sns_client.send_message(f"Closing dead task: {task[_('task_id')]} ", subject='SOSW Dead Task')
            self.task_client.archive_task(task[_('task_id')])


    def should_retry_task(self, labourer: Labourer, task: Dict) -> bool:
        attempts = task.get(self.get_db_field_name('attempts'))
        return True if attempts < labourer.max_attempts else False


    def move_task_to_retry_table(self, task: Dict, labourer: Labourer):
        """
        Put the task to a Dynamo table `sosw_retry_tasks`, with the wanted delay: labourer.max_runtime * attempts.
        Delete it from `sosw_tasks` table.
        """

        wanted_delay = self.calculate_delay_for_task_retry(labourer, task)
        self.task_client.move_task_to_retry_table(task, wanted_delay)


    def calculate_delay_for_task_retry(self, labourer: Labourer, task: Dict) -> int:
        attempts = task[self.get_db_field_name('attempts')]
        wanted_delay = labourer.max_duration * attempts
        return wanted_delay


    def retry_tasks(self):
        """
        Read from dynamo table `sosw_retry_tasks`, get tasks with retry_time <= now, and put them to `sosw_tasks` in the
        beginning of the queue.
        """

        tasks_to_retry = self.task_client.get_tasks_to_retry_for_labourer(limit=self.config.get('retry_tasks_limit'))
        self.task_client.retry_tasks(tasks_to_retry)


    def get_db_field_name(self, key: str) -> str:
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.config['dynamo_db_config']['field_names'].get(key, key)
