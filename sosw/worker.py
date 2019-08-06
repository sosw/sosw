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
    We recommend that you inherit ``YOUR_FUNCTION.Processor`` from this class in Lambdas
    that are orchestrated by ``sosw``.

    Worker has all the common methods of :ref:`Processor` and tries to mark task as completed if received
    ``task_id`` in the ``event``.

    The ``__call__`` method is supposed to accept the ``event`` of the Lambda invocation.
    This is a dictionary with the payload received in the ``lambda_handler`` during invocation.

    We encourage you to call ``super()`` at **the end** of your child function in order to collects stats
    and report to WorkerAssistant. It is important not to call it in the beginning of your worker before the
    actual work is done.

    In case your worker function needs to return something - you probably **should not** inherit from `Worker`.
    Basic stats aggregation is performed by :ref:`Processor` so calling it's ``super()`` will do the job.

    .. note::
       Make sure that ``YOUR_FUNCTION`` has permissions to call AWS Lambda API to invoke ``sosw_worker_assistant``!

    """

    DEFAULT_CONFIG = {
        'init_clients':                 ['lambda'],
        'sosw_worker_assistant_lambda': 'sosw_worker_assistant'
    }

    # these clients will be initialized by Processor constructor
    lambda_client = None


    def __call__(self, event: Dict):

        super().__call__(event)

        # Mark the task as completed in DynamoDB if the event had task_id.
        task_id = event.get('task_id')
        if task_id:
            self.mark_task_as_completed(event.get('task_id'))



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
