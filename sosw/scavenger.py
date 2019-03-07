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


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Scavenger(Processor):
    DEFAULT_CONFIG = {
        'init_clients': ['Task', 'Ecology', 'Sns', 'DynamoDb'],
        'sns_config': {
            'recipient': 'arn:aws:sns:us-west-2:0000000000:sosw_info',
            'subject':   'SOSW Info'
        },
    }

    # these clients will be initialized by Processor constructor
    task_client = None
    ecology_client = None
    sns_client = None
    dynamo_db_client = None


    def __call__(self):
        # Get labourers
        labourers = self.task_client.register_labourers()

        # Handle expired tasks - close or allow to retry
        for name, labourer in labourers.items():
            expired_tasks = self.task_client.get_expired_tasks_for_labourer(labourer)
            if expired_tasks:
                labourer_health = self.ecology_client.get_labourer_status(labourer)

                for task in expired_tasks:
                    self.process_expired_task(task, labourer_health)

        # Archive closed tasks
        for name, labourer in labourers.items():
            closed_tasks = self.task_client.get_closed_tasks_for_labourer(labourer)

            for task in closed_tasks:
                task_id = task[self.get_db_field_name('task_id')]
                self.task_client.archive_task(task_id)


    def process_expired_task(self, task: Dict, labourer_health: int):
        _ = self.get_db_field_name

        if self.should_retry_task(task, labourer_health):
            self.allow_task_to_retry(task)
        else:
            self.sns_client.send_message(f"Closing dead task: {task[_('task_id')]} ", subject='SOSW Dead Task')
            self.task_client.close_task(task[_('task_id')])


    def should_retry_task(self, task: Dict, labourer_health: int) -> bool:
        _ = self.get_db_field_name

        attempts = task.get(_('attempts'))
        created_at = task.get(_('created_at'))
        labourer_id = task.get(_('labourer_id'))

        if created_at < time.time() - self.config.get('max_retry_delta_ms'):
            return False

        if self.config.get(f'max_attempts_{labourer_id}') \
                and attempts > self.config.get(f'max_attempts_{labourer_id}'):
            return False
        elif attempts > self.config.get('max_attempts'):
            return False

        if labourer_health < self.config.get('min_labourer_health'):
            return False

        return True


    def allow_task_to_retry(self, task: Dict):
        """
        Puts the task at the beginning of the queue and updates task's attempts.
        It will no longer be considered as expired.
        """

        _ = self.get_db_field_name

        new_greenfield = self.recalculate_greenfield(task)

        self.dynamo_db_client.update(
                {_('task_id'): task[_('task_id')], _('labourer_id'): task[_('labourer_id')]},
                attributes_to_update={_('greenfield'): new_greenfield},
                attributes_to_increment={_('attempts'): 1}
        )


    def recalculate_greenfield(self, task: Dict):
        raise NotImplementedError


    def get_db_field_name(self, key: str) -> str:
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.config['dynamo_db_config']['field_names'].get(key, key)
