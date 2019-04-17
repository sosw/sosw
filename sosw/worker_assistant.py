import time

from sosw import Processor
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import get_one_from_dict


class WorkerAssistant(Processor):
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
                'function': self.mark_task_as_completed,
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
