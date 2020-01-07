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
    ``task_id`` in the ``event``. Worker create a payload with ``stats`` and ``result`` if exist and invoke worker
    assistant lambda.
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
            if event.get('task_id'):
                self.mark_task_as_completed(event['task_id'])
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
            'task_id': task_id,
        }

        if self.stats:
            payload.update({'stats': self.stats})

        if self.result:
            payload.update({'result': self.result})

        payload = json.dumps(payload)

        lambda_response = self.lambda_client.invoke(
                FunctionName=worker_assistant_lambda_name,
                InvocationType='Event',
                Payload=payload
        )
        logger.debug(f"mark_task_as_completed response: {lambda_response}")
