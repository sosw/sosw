"""
Tests of the ``DynamoDbClient.__init__`` logic.

These tests patch attributes of the ``DynamoDbClient`` class itself (e.g. ``enrich_config_from_glue``),
so we keep them in a separate module from the main ``test_dynamo_db``. All class patching here goes
strictly through ``unittest.mock.patch.object`` context managers, so the class is guaranteed to be
restored even if a test fails and the module is safe to register in the common suite.
Approved by @ngr
"""

import logging
import unittest
import os

from copy import deepcopy
from unittest.mock import MagicMock, patch

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from ...dynamo_db import DynamoDbClient


class dynamodb_client_init_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = {
        'row_mapper':      {
            'lambda_name':   'S',
            'invocation_id': 'S',
            'en_time':       'N',

            'hash_col':      'S',
            'range_col':     'N',
            'other_col':     'S',
            'new_col':       'S',
            'some_col':      'S',
            'some_counter':  'N',
            'some_bool':     'BOOL',
            'some_bool2':    'BOOL',
            'some_map':      'M',
            'some_list':     'L'
        },
        'required_fields': ['lambda_name'],
        'table_name':      'autotest_dynamo_db',
        'hash_key':        'hash_col',
    }


    def setUp(self):
        self.patcher = patch("boto3.client")
        self.paginator_mock = MagicMock()
        self.dynamo_mock = MagicMock()
        self.dynamo_mock.get_paginator.return_value = self.paginator_mock

        self.boto3_client_patch = self.patcher.start()
        self.boto3_client_patch.return_value = self.dynamo_mock


    def tearDown(self):
        self.patcher.stop()


    def test_init__config_must_be_a_dictionary(self):
        for bad_config in [None, 'autotest_dynamo_db', ['boo'], 42]:
            self.assertRaises(AssertionError, DynamoDbClient, bad_config)


    def test_init__bad_table_name_in_autotest__raises(self):
        config = deepcopy(self.TEST_CONFIG)
        config['table_name'] = 'production_table'

        with self.assertRaises(AssertionError) as e:
            DynamoDbClient(config=config)

        self.assertIn("Bad table name production_table in autotest", str(e.exception))


    def test_init__skip_glue__uses_config_as_is(self):
        config = {'skip_glue': True, **deepcopy(self.TEST_CONFIG)}

        with patch.object(DynamoDbClient, 'enrich_config_from_glue') as glue_mock:
            client = DynamoDbClient(config=config)

        glue_mock.assert_not_called()
        self.assertIs(client.config, config)
        self.assertEqual(client.row_mapper, config['row_mapper'])


    def test_enrich_config_from_glue__call_logic(self):
        TESTS = [
            ({
                 'config':      deepcopy(self.TEST_CONFIG),
                 'glue_client': True,
             },
             1),
            ({
                 'config': deepcopy(self.TEST_CONFIG),
             },
             1),
            ({
                 'config':      {'skip_glue': True, **deepcopy(self.TEST_CONFIG)},
                 'glue_client': True,
             },
             0),
            ({
                 'config': {'skip_glue': True, **deepcopy(self.TEST_CONFIG)}},
             0),
        ]

        original_method = DynamoDbClient.enrich_config_from_glue

        for init_payload, expected_calls in TESTS:
            with patch.object(DynamoDbClient, 'enrich_config_from_glue',
                              return_value=deepcopy(self.TEST_CONFIG)) as glue_mock:
                DynamoDbClient(**init_payload)
                self.assertEqual(len(glue_mock.mock_calls), expected_calls)

        self.assertIs(DynamoDbClient.enrich_config_from_glue, original_method,
                      "The mocked method must be restored on the class to keep other tests of the suite safe")


if __name__ == '__main__':
    unittest.main()
