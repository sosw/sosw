import time

from sosw import Processor
from sosw.components.dynamo_db import DynamoDbClient


class WorkerAssistant(Processor):
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
                'payload':             'S'
            },
            'required_fields':  ['task_id', 'labourer_id', 'created_at', 'greenfield'],

            'field_names':      {}
        }
    }

    # these clients will be initialized by Processor constructor
    dynamo_db_client: DynamoDbClient = None


    def mark_task_as_completed(self, task_id: str):
        _ = self.get_db_field_name

        self.dynamo_db_client.update(
                keys={_('task_id'): task_id},
                attributes_to_update={_('completed_at'): int(time.time())},
        )


    def get_db_field_name(self, field: str) -> str:
        mapping = self.config['dynamo_db_config'].get('field_names', {})
        return mapping.get(field, field)
