__all__ = ['Scavenger']

__author__ = "Sophie Fogel"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

import logging
from typing import Dict
import time

from sosw.app import Processor
from sosw.labourer import Labourer


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Scavenger(Processor):
    DEFAULT_CONFIG = {
        'init_clients': ['Task', 'Ecology', 'Sns', 'DynamoDb'],
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
        }
    }

    # these clients will be initialized by Processor constructor
    task_client = None
    ecology_client = None
    sns_client = None
    dynamo_db_client = None


    def __call__(self):
        labourers = self.task_client.register_labourers()

        for labourer in labourers:
            self.handle_expired_tasks_for_labourer(labourer)

        for labourer in labourers:
            self.archive_closed_tasks_for_labourer(labourer)


    def handle_expired_tasks_for_labourer(self, labourer: Labourer):
        expired_tasks = self.task_client.get_expired_tasks_for_labourer(labourer)
        if expired_tasks:
            for task in expired_tasks:
                self.process_expired_task(task, labourer)


    def archive_closed_tasks_for_labourer(self, labourer: Labourer):
        closed_tasks = self.task_client.get_closed_tasks_for_labourer(labourer)

        for task in closed_tasks:
            task_id = task[self.get_db_field_name('task_id')]
            self.task_client.archive_task(task_id)


    def process_expired_task(self, labourer: Labourer, task: Dict):
        _ = self.get_db_field_name

        if self.should_retry_task(labourer, task):
            self.allow_task_to_retry(task)
        else:
            self.sns_client.send_message(f"Closing dead task: {task[_('task_id')]} ", subject='SOSW Dead Task')
            self.task_client.close_task(task[_('task_id')], completed=False)


    @staticmethod
    def should_retry_task(labourer: Labourer, task: Dict) -> bool:
        attempts = task.get(_('attempts'))

        if hasattr(labourer, 'min_health_for_retry') and hasattr(labourer, 'health') \
                and labourer.health < labourer.min_health_for_retry:
            return False

        return True if attempts < labourer.max_attempts else False


    def allow_task_to_retry(self, task: Dict):
        """
        Puts the task at the beginning of the queue and updates task's attempts.
        It will no longer be considered as expired.
        """

        _ = self.get_db_field_name

        new_greenfield = self.recalculate_greenfield(task)

        self.dynamo_db_client.update(
                {_('task_id'): task[_('task_id')], _('labourer_id'): task[_('labourer_id')]},
                attributes_to_update={_('greenfield'): new_greenfield}
        )


    def recalculate_greenfield(self, task: Dict):
        raise NotImplementedError


    def get_db_field_name(self, key: str) -> str:
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.config['dynamo_db_config']['field_names'].get(key, key)
