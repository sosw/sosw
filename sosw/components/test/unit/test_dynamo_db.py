import boto3
import logging
import time
import unittest
import os

from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.dynamo_db import DynamoDbClient


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
            'some_counter':  'N'
        },
        'required_fields': ['lambda_name'],
        'table_name':      'autotest_dynamo_db'
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


    def tearDown(self):
        self.patcher.stop()


    def test_dict_to_dynamo_strict(self):
        dict_row = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456}
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row)
        expected = {'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'}}
        for key in expected.keys():
            self.assertDictEqual(expected[key], dynamo_row[key])


    def test_dict_to_dynamo_not_strict(self):
        dict_row = {'name': 'cat', 'age': 3}
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row, strict=False)
        expected = {'name': {'S': 'cat'}, 'age': {'N': '3'}}
        for key in expected.keys():
            self.assertDictEqual(expected[key], dynamo_row[key])


    def test_dict_to_dynamo_prefix(self):
        dict_row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no'}
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row, add_prefix="#")
        expected = {'#hash_col': {'S': 'cat'}, '#range_col': {'N': '123'}, '#some_col': {'S': 'no'}}
        for key in expected.keys():
            self.assertDictEqual(expected[key], dynamo_row[key])


    def test_dynamo_to_dict(self):
        dynamo_row = {
            'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
            'extra_key':   {'N': '42'}
        }
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row)
        expected = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456}
        self.assertDictEqual(dict_row, expected)


    def test_dynamo_to_dict_no_strict_row_mapper(self):
        dynamo_row = {
            'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
            'extra_key_n': {'N': '42'}, 'extra_key_s': {'S': 'wowie'}
        }
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=False)
        expected = {
            'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456, 'extra_key_n': 42,
            'extra_key_s': 'wowie'
        }
        self.assertDictEqual(dict_row, expected)


    def test_dynamo_to_dict__dont_json_loads(self):
        config = self.TEST_CONFIG.copy()
        config['dont_json_loads_results'] = True

        self.dynamo_client = DynamoDbClient(config=config)

        dynamo_row = {
            'hash_col':   {'S': 'aaa'}, 'range_col': {'N': '123'}, 'other_col': {'S': '{"how many": 300}'},
            'duck_quack': {'S': '{"quack": "duck"}'}
        }
        res = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=False)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': '{"how many": 300}', 'duck_quack': '{"quack": "duck"}'
        }
        self.assertDictEqual(res, expected)

        res = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=True)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': '{"how many": 300}'
        }
        self.assertDictEqual(res, expected)


    def test_dynamo_to_dict__do_json_loads(self):
        config = self.TEST_CONFIG.copy()
        config['dont_json_loads_results'] = False

        self.dynamo_client = DynamoDbClient(config=config)

        dynamo_row = {
            'hash_col':   {'S': 'aaa'}, 'range_col': {'N': '123'}, 'other_col': {'S': '{"how many": 300}'},
            'duck_quack': {'S': '{"quack": "duck"}'}
        }
        res = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=False)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': {"how many": 300}, 'duck_quack': {"quack": "duck"}
        }
        self.assertDictEqual(res, expected)

        res = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=True)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': {"how many": 300}
        }
        self.assertDictEqual(res, expected)



    def test_get_by_query__validates_comparison(self):
        self.assertRaises(AssertionError, self.dynamo_client.get_by_query, keys={'k': '1'},
                          comparisons={'k': 'unsupported'})


    def test_get_by_query__between(self):
        keys = {'hash_col': 'cat', 'st_between_range_col': '3', 'en_between_range_col': '6'}

        self.dynamo_client = DynamoDbClient(config=self.TEST_CONFIG)

        self.dynamo_client.get_by_query(keys=keys)
        # print(f"Call_args for paginate: {self.paginator_mock.paginate.call_args}")

        args, kwargs = self.paginator_mock.paginate.call_args
        # print(kwargs)

        self.assertEqual(len(kwargs['ExpressionAttributeValues']), 3)
        self.assertIn('range_col between :st_between_range_col and :en_between_range_col',
                      kwargs['KeyConditionExpression'])


    def test_get_by_query__return_count(self):

        # Make sure dynamo paginator is mocked.
        self.paginator_mock.paginate.return_value = [{'Count': 24, 'LastEvaluatedKey': 'bzz'}, {'Count': 12}]
        self.dynamo_client.dynamo_client.get_paginator.return_value = self.paginator_mock

        # Call the manager
        result = self.dynamo_client.get_by_query(keys={'a': 'b'}, return_count=True)

        # Validate result
        self.assertEqual(result, 36, f"Result from 2 pages should be 24 + 12, but we received: {result}")

        # Make sure the paginator was called
        self.dynamo_client.dynamo_client.get_paginator.assert_called()


    def test__parse_filter_expression(self):
        TESTS = {
            'key = 42': ("key = :filter_key", {":filter_key": {'N': '42'}}),
            '   key    = 42  ': ("key = :filter_key", {":filter_key": {'N': '42'}}),
            'cat = meaw': ("cat = :filter_cat", {":filter_cat": {'S': 'meaw'}}),
            'magic between 41 and 42': ("magic between :st_between_magic and :en_between_magic",
                                        {":st_between_magic": {'N': '41'}, ":en_between_magic": {'N': '42'}}),
            'attribute_not_exists boo': ("attribute_not_exists (boo)", {})
        }

        for data, expected in TESTS.items():
            self.assertEqual(self.dynamo_client._parse_filter_expression(data), expected)


    def test__parse_filter_expression__raises(self):

        TESTS = [
            {'k': 1}, [1,2], None,  # Invalid input types
            'key == 42', 'foo ~ 1', 'foo3 <=> 0', 'key between 42',  # Invalid operators
            'key between 23, 25', 'key between [23, 25]', 'key 23 between 21',  # Invalid between formats.
        ]

        for data in TESTS:
            self.assertRaises((AssertionError, ValueError), self.dynamo_client._parse_filter_expression, data)


    def test_create__calls_put(self):
        row = {'hash_col': 'cat', 'range_key': 'test', 'another_col': 'wow'}
        self.dynamo_client.put = MagicMock(return_value=None)

        self.dynamo_client.create(row)

        self.dynamo_client.put.assert_called_once_with(row, None, overwrite_existing=False)


if __name__ == '__main__':
    unittest.main()
