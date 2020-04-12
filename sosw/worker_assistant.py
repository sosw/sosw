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


__all__ = ['WorkerAssistant']
__author__ = "Sophie Fogel"
__version__ = "1.0"


import json
import time

from sosw.essential import Essential
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import get_one_from_dict
from typing import Dict


class WorkerAssistant(Essential):
    """
    Worker Assistant is the interface Worker Lambdas should call to mark their tasks completed.

    The future versions will also accept the remaining workload to process to call Siblings in case the worker is
    about to time out and wants to finish healthy.

    This Essential is supposed to be called synchronously by the Worker Lambdas.
    Should pass the ``action`` and ``task_id`` attributes in the payload of the call.

    See example of the usage in :ref:`Worker`.
    """

    DEFAULT_CONFIG = {
        'init_clients':     ['DynamoDb'],
        'dynamo_db_config': {
            'table_name':       'sosw_tasks',
            'index_greenfield': 'sosw_tasks_greenfield',
            'row_mapper':       {
                'task_id':             'S',
                'labourer_id':         'S',
                'created_at':          'N',
                'completed_at':        'N',
                'greenfield':          'N',
                'attempts':            'N',
                'closed_at':           'N',
                'desired_launch_time': 'N',
                'arn':                 'S',
                'payload':             'S',
                'stats':               'M',
                'result':              'S',
            },
            'required_fields':  ['task_id', 'labourer_id', 'created_at', 'greenfield'],

            'field_names':      {}
        }
    }

    # these clients will be initialized by Processor constructor
    dynamo_db_client: DynamoDbClient = None


    def __call__(self, event):
        action = get_one_from_dict(event, 'action', str)

        mapper = {
            'mark_task_as_completed': {
                'function':        self.mark_task_as_completed,
                'required_params': ['task_id']
            }
        }

        if action in mapper:
            func = mapper[action]['function']
            required_params = mapper[action]['required_params']

            for req_param in required_params:
                if req_param not in event:
                    raise Exception(f"Missing required parameter `{req_param}` in event for action `{action}`")

            func_kwargs = {k: event[k] for k in event if k in required_params}

            if 'stats' in event:
                if isinstance(event['stats'], str):
                    func_kwargs.update({'stats': json.loads(event['stats'])})
                else:
                    func_kwargs.update({'stats': event['stats']})

            if 'result' in event:
                if isinstance(event['result'], str):
                    func_kwargs.update({'result': json.loads(event['result'])})
                else:
                    func_kwargs.update({'result': event['result']})

            return func(**func_kwargs)
        else:
            raise Exception(f"Action `{action}` is not supported")


    def mark_task_as_completed(self, task_id: str, stats: Dict = None, result: Dict = None):
        assert isinstance(task_id, str), f"`task_id` must be a string"

        _ = self.get_db_field_name

        fields_to_update = {_('completed_at'): int(time.time())}

        if stats:
            fields_to_update.update({f'stat_{k}': v for k, v in stats.items()})

        if result:
            fields_to_update.update({f'result_{k}': v for k, v in result.items()})

        self.dynamo_db_client.update(
            keys={_('task_id'): task_id},
            attributes_to_update=fields_to_update,
        )


    def get_db_field_name(self, field: str) -> str:
        mapping = self.config['dynamo_db_config'].get('field_names', {})
        return mapping.get(field, field)
