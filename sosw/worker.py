"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers
    Copyright (C) 2019  sosw core contributors

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/gpl-3.0.html>.
"""

__all__ = ['Worker']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import json
import logging

from sosw.app import Processor
from typing import Dict


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
        'init_clients':                 ['lambda'],
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
        except Exception:
            logger.exception(f"Failed to call WorkerAssistant for event {event}")
            pass

        super().__call__(event)


    def mark_task_as_completed(self, task_id: str):
        """ Call worker assistant lambda and tell it to close task """

        if not self.lambda_client:
            self.register_clients(['lambda'])

        worker_assistant_lambda_name = self.config.get('sosw_worker_assistant_lambda', 'sosw_worker_assistant')
        payload = {
            'action':  'mark_task_as_completed',
            'task_id': task_id
        }
        payload = json.dumps(payload)

        lambda_response = self.lambda_client.invoke(
                FunctionName=worker_assistant_lambda_name,
                InvocationType='Event',
                Payload=payload
        )
        logger.debug(f"mark_task_as_completed response: {lambda_response}")
