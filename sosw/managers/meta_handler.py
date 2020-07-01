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

__all__ = ['MetaHandler']
__author__ = "Mark Bulgakov, Nikolay Grishchenko"
__version__ = "1.0"

import logging
import os
import time


from sosw.app import global_vars
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import recursive_update
from typing import Dict


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MetaHandler:
    """
    MetaHandler is helper class for Essential classes.
    It works with DynamoDB table to store the meta data of operations on Tasks.
    """

    DEFAULT_CONFIG = {
        'write_meta_to_ddb': True,
        'dynamo_db_config': {
            'table_name': 'sosw_tasks_meta',
            'row_mapper': {
                'task_id': 'S',
                'created_at': 'N',
                'author': 'S',
                'invocation_id': 'S',
                'log_stream_name': 'S',
                'action': 'S'
            },
            'required_fields': [
                'task_id',
                'created_at',
                'author',
                'invocation_id',
                'log_stream_name',
                'action'
            ],
        },
    }

    AVAILABLE_ACTIONS_MAPPING = {}
    CONTEXT_FIELDS_MAPPINGS = {
        'author': 'function_name',
        'invocation_id': 'aws_request_id',
        'log_stream_name': 'log_stream_name'
    }

    def __init__(self, custom_config: Dict = None, **kwargs):

        # Initialize config from default config
        self.config = self.DEFAULT_CONFIG or {}

        # Update config recursively from custom config
        self.config = recursive_update(self.config, custom_config or {})

        if self.config['write_meta_to_ddb']:
            try:
                self.dynamo_db_client = DynamoDbClient(config=self.config['dynamo_db_config'])
            except:
                logging.exception("Failed to initialize MetaHandler DynamoDbClient")
                self.dynamo_db_client = None
        else:
            self.dynamo_db_client = None


    def post(self, task_id: str, action: str, **kwargs):
        """
        Write row with meta data to sosw_tasks_meta DynamoDB Table if configured.
        As long as collecting the meta data is optional, the ``MetaHandler`` will either save it
        to DynamoDB or just log.
        """

        row = {
            'task_id': task_id,
            'created_at': time.time(),
            'action': self._ma(action)
        }

        assert not set(kwargs).intersection(set(self.CONTEXT_FIELDS_MAPPINGS)), \
            'args from lambda_context should not be passed'

        for field, value in kwargs.items():
            row[field] = value

        for field, mapping in self.CONTEXT_FIELDS_MAPPINGS.items():
            row[field] = global_vars.lambda_context[mapping]

        if self.dynamo_db_client:
            self.dynamo_db_client.create(row=row)
        else:
            logger.info("DynamoDB client/table is not configured for meta_handler. Skip saving task meta data: %", row)

    def _ma(self, field_name):
        # FIXMEONEDAY: implement mappings for the actions names
        return str(field_name)
