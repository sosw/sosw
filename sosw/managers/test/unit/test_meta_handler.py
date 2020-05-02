import boto3
import logging
import os
import unittest

from copy import deepcopy
from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components import dynamo_db
from sosw.managers.meta_handler import MetaHandler
from sosw.test.variables import TEST_META_HANDLER_CONFIG


class meta_handler_UnitTestCase(unittest.TestCase):

    def setUp(self):

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.config = deepcopy(TEST_META_HANDLER_CONFIG)

        with patch('boto3.client'):
            self.manager = MetaHandler(custom_config=self.config)

        self.manager.dynamo_db_client = MagicMock(spec=dynamo_db.DynamoDbClient)


    def tearDown(self):
        self.patcher.stop()


    def test_post__raise_kwargs_intersection_with_lambda_context(self):
        task_id = 'test_task_id'
        action = 'archive_task'

        self.assertRaises(AssertionError, self.manager.post, **{'task_id': task_id, 'action': action,
                                                                'invocation_id': 'test_invocation_id'})


    def test_ma__return_string(self):
        self.assertEqual(type(self.manager._ma(123)), str)
