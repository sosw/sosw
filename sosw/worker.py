import json
import logging

from sosw.app import Processor
from typing import Dict

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
    We recommend that you inherit your core Processor from this class in Lambdas that are orchestrated by `sosw`.

    The ``__call__`` method is supposed to accept the ``event`` of the Lambda invocation.
    This is a dictionary with the payload received in the lambda_handler during invocation.

    Worker has all the common methods of :ref:`Processor` and tries to mark task as completed if received
    ``task_id`` in the ``event``.
    """

    DEFAULT_CONFIG = {
        'init_clients': ['lambda'],
        'sosw_worker_assistant_lambda': 'sosw_worker_assistant'
    }

    # these clients will be initialized by Processor constructor
    lambda_client = None

    def __call__(self, event: Dict):
        """
        You can either call super() at the end of your child function or completely overwrite this function.
        """

        # Mark the task as completed in DynamoDB if the event had task_id.
        try:
            self.mark_task_as_completed(event.get('task_id'))
        except:
            logger.exception(f"Failed to call WorkerAssistant for event {event}")
            pass

        super().__call__(event)


    def mark_task_as_completed(self, task_id: str):
        """ Call worker assistant lambda and tell it to close task """

        if not self.lambda_client:
            self.register_clients(['lambda'])

        worker_assistant_lambda_name = self.config.get('sosw_worker_assistant_lambda', 'sosw_worker_assistant')
        payload = {
            'action': 'mark_task_as_completed',
            'task_id': task_id
        }
        payload = json.dumps(payload)

        lambda_response = self.lambda_client.invoke(
                FunctionName=worker_assistant_lambda_name,
                InvocationType='Event',
                Payload=payload
        )
        logger.debug(f"mark_task_as_completed response: {lambda_response}")
