import datetime
import logging
import os
import unittest

from copy import deepcopy
from unittest.mock import MagicMock, patch

from sosw.app import global_vars
from sosw.components import dynamo_db
from sosw.managers.meta_handler import MetaHandler
from sosw.test.variables import TEST_META_HANDLER_CONFIG, TEST_META_HANDLER_LAMBDA_CONTEXT, TEST_META_HANDLER_POST_ARGS


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class meta_handler_UnitTestCase(unittest.TestCase):

    CONTEXT_FIELDS = {v: k for k, v in MetaHandler.CONTEXT_FIELDS_MAPPINGS.items()}


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.config = deepcopy(TEST_META_HANDLER_CONFIG)

        with patch('boto3.client'):
            global_vars.lambda_context = self.CONTEXT_FIELDS
            self.manager = MetaHandler(custom_config=self.config)

        self.manager.dynamo_db_client = MagicMock(spec=dynamo_db.DynamoDbClient)
        self.manager.lambda_context = TEST_META_HANDLER_LAMBDA_CONTEXT
        self.expected_values = {
            'task_id':         TEST_META_HANDLER_POST_ARGS['task_id'],
            'action':          self.manager._ma(TEST_META_HANDLER_POST_ARGS['action']),
            'author':          'author',
            'invocation_id':   'invocation_id',
            'log_stream_name': 'log_stream_name'
        }

    def tearDown(self):
        self.patcher.stop()


    def test_post__raise_kwargs_intersection_with_lambda_context(self):
        """
        Method should raise in case attempt to overwrite lambda_context fields from kwargs
        """

        kwargs = {'invocation_id': 'test_invocation_id'}
        self.assertRaises(AssertionError, self.manager.post, **TEST_META_HANDLER_POST_ARGS, **kwargs)


    def test_post__check_create_call(self):
        """
        Check that method will try to create correct row
        """

        self.manager.post(**TEST_META_HANDLER_POST_ARGS)
        self.manager.dynamo_db_client.create.assert_called_once()

        args, kwargs = self.manager.dynamo_db_client.create.call_args
        for k, v in self.expected_values.items():
            self.assertEqual(v, kwargs['row'][k])


    def test_post__check_create_call_with_args_from_kwargs(self):
        """
        Check that method will try to create correct row with kwargs
        """

        kwargs = {
            'result':        'test_results',
            'stats':         'test_stats',
            'health_status': 'healthy'
        }

        expected_row = deepcopy(self.expected_values)
        expected_row.update(kwargs)

        self.manager.post(**TEST_META_HANDLER_POST_ARGS, **kwargs)
        self.manager.dynamo_db_client.create.assert_called_once()

        args, kwargs = self.manager.dynamo_db_client.create.call_args
        for k, v in self.expected_values.items():
            self.assertEqual(v, kwargs['row'][k])


        self.assertEqual(type(self.manager._ma(123)), str)
