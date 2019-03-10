import os
import unittest
from copy import deepcopy

from unittest.mock import Mock

from sosw.scavenger import Scavenger
from sosw.test.variables import TEST_SCAVENGER_CONFIG
from sosw.components.dynamo_db import DynamoDbClient


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scavenger_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = deepcopy(TEST_SCAVENGER_CONFIG)

    def setUp(self):
        self.TEST_CONFIG['init_clients'] = ['DynamoDb']

        self.scavenger = Scavenger(self.TEST_CONFIG)
        self.dynamo_client = DynamoDbClient(config=self.TEST_CONFIG['dynamo_db_config'])

        self.table_name = self.TEST_CONFIG['dynamo_db_config']['table_name']

        _ = self.scavenger.get_db_field_name

        self.task = {
            _('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): 8888, _('attempts'): 2
        }


    def test_allow_task_to_retry(self):
        _ = self.scavenger.get_db_field_name

        self.scavenger.recalculate_greenfield = Mock(return_value=9999)

        self.dynamo_client.put(self.task, self.table_name)

        self.scavenger.allow_task_to_retry(deepcopy(self.task))

        tasks = self.dynamo_client.get_by_query(keys={_('task_id'): '123', _('labourer_id'): 'lambda1'})
        self.assertEqual(len(tasks), 1)
        task = tasks[0]

        expected_task = deepcopy(self.task)
        expected_task[_('attempts')] = 3
        expected_task[_('greenfield')] = 9999

        self.assertDictEqual(task, expected_task)
