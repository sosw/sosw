import boto3
import logging
import os

from importlib import import_module
from collections import defaultdict

from sosw.app import Processor
from sosw.managers.task import TaskManager


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

__all__ = ['Orchestrator']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class WorkerObj:
    id = None


    def __init__(self, **kwargs):
        for k, v in kwargs:
            setattr(self, k, v)


class Orchestrator(Processor):
    """
    Orchestrator class.
    """

    DEFAULT_CONFIG = {
        'init_clients':                  ['Task'],
        'invocation_number_coefficient': {
            0:   [0, 1],
            0.5: [2],
            1:   [3, 4]
        },
        'workers':                       {}
    }


    def __call__(self, event):
        workers = self.get_workers()
        for worker in workers:
            self.process_worker(worker.id)


    def process_worker(self, worker_id: int):
        number_of_tasks = self.get_desired_invocation_number_for_worker(worker_id=worker_id)

        tasks_to_process = self.task_client.get_next_for_worker(worker_id=worker_id, cnt=number_of_tasks)
        logger.info(tasks_to_process)

        self.invoke_worker()


    def get_worker_setting(self, worker_id: int, attribute: str):
        """ Should probably try to use some default values, but for now we delegate this to whoever calls me. """

        try:
            return self.config['workers'][worker_id][attribute]
        except KeyError:
            return None


    def get_desired_invocation_number_for_worker(self, worker_id: int) -> int:
        worker_status = self.ecology_client.get_worker_status(worker_id=worker_id)

        coefficient = next(k for k, v in self.config['invocation_number_coefficient'] if worker_status in v)


    def get_workers(self):
        return [WorkerObj(id=1)]
