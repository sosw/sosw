import logging

from sosw.app import Processor
from sosw.components.tasks_api_client_for_workers import close_task


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Production"


__all__ = ['Worker']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Worker(Processor):
    """
    Worker class template.
    """

    def __call__(self, event):
        """
        Call the Worker Processor.
        You can either call super() at the end of your child function or completely overwrite this function.
        """

        # Mark the task as completed in DynamoDB if the event had task_id.
        try:
            close_task(event)
        except:
            pass

        super().__call__(event)
