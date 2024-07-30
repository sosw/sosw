"""
This single test we have to move to a separate module, because after mocking the class code object,
we didn't find a way to reimport it back normally, and it keeps the artifacts for future tests in the TestCase.
Approved by @ngr
"""

import datetime
import gc
import logging
import sys
import time
import unittest
import os

from copy import deepcopy
from decimal import Decimal
from unittest.mock import MagicMock, patch, Mock

from .helpers_test_variables import PPR_DESCRIBE_TABLE, PT_DESCRIBE_TABLE

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from ...dynamo_db import DynamoDbClient


class dynamodb_client_UnitTestCase(unittest.TestCase):
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
		# 'skip_glue': True,
	}


	def setUp(self):
		self.HASH_KEY = ('hash_col', 'S')
		self.RANGE_KEY = ('range_col', 'N')
		self.KEYS = ('hash_col', 'range_col')
		self.table_name = 'autotest_dynamo_db'

		self.patcher = patch("boto3.client")
		self.paginator_mock = MagicMock()
		self.dynamo_mock = MagicMock()
		self.dynamo_mock.get_paginator.return_value = self.paginator_mock

		self.boto3_client_patch = self.patcher.start()
		self.boto3_client_patch.return_value = self.dynamo_mock

		self.dynamo_client = DynamoDbClient(config=self.TEST_CONFIG)


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

		for init_payload, expected_calls in TESTS:
			TestDynamoDbClient = deepcopy(DynamoDbClient)
			TestDynamoDbClient.enrich_config_from_glue = MagicMock(return_value=deepcopy(self.TEST_CONFIG))
			test_client = TestDynamoDbClient(**init_payload)
			self.assertEqual(len(test_client.enrich_config_from_glue.mock_calls), expected_calls)


if __name__ == '__main__':
	unittest.main()
