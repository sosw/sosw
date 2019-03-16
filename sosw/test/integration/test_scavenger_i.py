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


    # def test_put_task_to_retry_table(self):
    #     _ = self.scavenger.get_db_field_name
    #
    #     raise NotImplementedError
