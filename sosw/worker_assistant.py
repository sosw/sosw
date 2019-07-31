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

__all__ = ['WorkerAssistant']
__author__ = "Sophie Fogel"
__version__ = "1.0"

import time

from sosw import Processor
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import get_one_from_dict


class WorkerAssistant(Processor):
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
            'table_name':       'autotest_sosw_tasks',
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
                'payload':             'S'
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
            return func(**func_kwargs)
        else:
            raise Exception(f"Action `{action}` is not supported")


    def mark_task_as_completed(self, task_id: str):
        assert isinstance(task_id, str), f"`task_id` must be a string"

        _ = self.get_db_field_name

        self.dynamo_db_client.update(
                keys={_('task_id'): task_id},
                attributes_to_update={_('completed_at'): int(time.time())},
        )


    def get_db_field_name(self, field: str) -> str:
        mapping = self.config['dynamo_db_config'].get('field_names', {})
        return mapping.get(field, field)
