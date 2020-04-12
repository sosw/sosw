import datetime
import logging
import time
import unittest
import os
from decimal import Decimal

from unittest.mock import MagicMock, patch, Mock


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
        dict_row = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456, 'some_bool': True,
                    'some_bool2': 'True', 'some_map': {'a': 1, 'b': 'b1', 'c': {'test': True}}, 'some_list': ['x', 'y']}
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row)
        expected = {
            'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
            'some_bool': {'BOOL': True}, 'some_bool2': {'BOOL': True},
            'some_map': {'M': {'a': {'N': '1'}, 'b': {'S': 'b1'}, 'c': {'M': {'test': {'BOOL': True}}}}},
            'some_list': {'L': [{'S': 'x'}, {'S': 'y'}]}
        }
        for key in expected.keys():
            self.assertDictEqual(expected[key], dynamo_row[key])


    def test_dict_to_dynamo_not_strict(self):
        dict_row = {'name': 'cat', 'age': 3, 'other_bool': False, 'other_bool2': 'False',
                    'other_map': {'a': 1, 'b': 'b1', 'c': {'test': True}}, 'some_list': ['x', 'y']}
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row, strict=False)
        expected = {'name': {'S': 'cat'}, 'age': {'N': '3'}, 'other_bool': {'BOOL': False},
                    'other_map': {'M': {'a': {'N': '1'}, 'b': {'S': 'b1'}, 'c': {'M': {'test': {'BOOL': True}}}}},
                    'some_list': {'L': [{'S': 'x'}, {'S': 'y'}]}}
        for key in expected.keys():
            self.assertDictEqual(expected[key], dynamo_row[key])


    def test_dict_to_dynamo__not_strict__map_type(self):
        dict_row = {
            'accept_mimetypes':     {'image/webp': 1, 'image/apng': 1, 'image/*': 1, '*/*': 0.8},
        }
        dynamo_row = self.dynamo_client.dict_to_dynamo(dict_row, strict=False)
        expected = {}
        logging.info(f"dynamo_row: {dynamo_row}")
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
            'extra_key':   {'N': '42'}, 'some_bool': {'BOOL': False},
            'some_map':    {'M': {'a': {'N': '1'}, 'b': {'S': 'b1'}, 'c': {'M': {'test': {'BOOL': True}}}}},
            'some_list':   {'L': [{'S': 'x'}, {'S': 'y'}]}
        }
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row)
        expected = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456, 'some_bool': False,
                    'some_map': {'a': 1, 'b': 'b1', 'c': {'test': True}}, 'some_list': ['x', 'y']}
        self.assertDictEqual(expected, dict_row)
        for k, v in dict_row.items():
            self.assertNotIsInstance(v, Decimal)
        for k, v in dict_row['some_map'].items():
            self.assertNotIsInstance(v, Decimal)


    def test_dynamo_to_dict_no_strict_row_mapper(self):
        dynamo_row = {
            'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
            'extra_key_n': {'N': '42'}, 'extra_key_s': {'S': 'wowie'}, 'other_bool': {'BOOL': True}
        }
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row, fetch_all_fields=True)
        expected = {
            'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456, 'extra_key_n': 42,
            'extra_key_s': 'wowie', 'other_bool': True
        }
        self.assertDictEqual(dict_row, expected)
        for k, v in dict_row.items():
            self.assertNotIsInstance(v, Decimal)


    def test_dynamo_to_dict__dont_json_loads(self):
        config = self.TEST_CONFIG.copy()
        config['dont_json_loads_results'] = True

        self.dynamo_client = DynamoDbClient(config=config)

        dynamo_row = {
            'hash_col':   {'S': 'aaa'}, 'range_col': {'N': '123'}, 'other_col': {'S': '{"how many": 300}'},
            'duck_quack': {'S': '{"quack": "duck"}'}
        }
        res = self.dynamo_client.dynamo_to_dict(dynamo_row, fetch_all_fields=True)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': '{"how many": 300}', 'duck_quack': '{"quack": "duck"}'
        }
        self.assertDictEqual(res, expected)

        res = self.dynamo_client.dynamo_to_dict(dynamo_row, fetch_all_fields=False)
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
        res = self.dynamo_client.dynamo_to_dict(dynamo_row, fetch_all_fields=True)
        expected = {
            'hash_col': 'aaa', 'range_col': 123, 'other_col': {"how many": 300}, 'duck_quack': {"quack": "duck"}
        }
        self.assertDictEqual(res, expected)

        res = self.dynamo_client.dynamo_to_dict(dynamo_row, fetch_all_fields=False)
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


    def test_get_by_query__expr_attr(self):
        keys = {'st_between_range_col': '3', 'en_between_range_col': '6', 'session': 'ses1'}
        expr_attrs_names = ['range_col', 'session']

        self.dynamo_client = DynamoDbClient(config=self.TEST_CONFIG)
        self.dynamo_client.get_by_query(keys=keys, expr_attrs_names=expr_attrs_names)

        args, kwargs = self.paginator_mock.paginate.call_args
        self.assertIn('#range_col', kwargs['ExpressionAttributeNames'])
        self.assertIn('#session', kwargs['ExpressionAttributeNames'])
        self.assertIn('#range_col between :st_between_range_col and :en_between_range_col AND #session = :session',
                      kwargs['KeyConditionExpression'])


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


    def test_batch_get_items_one_table__strict(self):
        # Strict - returns only fields that are in the row mapper
        db_items = [{'hash_col': {'S': 'b'}, 'range_col': {'N': '10'}, 'unknown_col': {'S': 'not_strict'}}]
        db_result = {'Responses': {'autotest_dynamo_db': db_items}}

        self.dynamo_client.dynamo_client.batch_get_item = Mock(return_value=db_result)

        result = self.dynamo_client.batch_get_items_one_table(keys_list=[{'hash_col': 'b'}], fetch_all_fields=False)

        self.assertEqual(result, [{'hash_col': 'b', 'range_col': 10}])


    def test_batch_get_items_one_table__not_strict(self):
        # Not strict - returns all fields
        db_items = [{'hash_col': {'S': 'b'}, 'range_col': {'N': '10'}, 'unknown_col': {'S': 'not_strict'}}]
        db_result = {'Responses': {'autotest_dynamo_db': db_items}}

        self.dynamo_client.dynamo_client.batch_get_item = Mock(return_value=db_result)

        result = self.dynamo_client.batch_get_items_one_table(keys_list=[{'hash_col': 'b'}], fetch_all_fields=True)

        self.assertEqual(result, [{'hash_col': 'b', 'range_col': 10, 'unknown_col': 'not_strict'}])


    def test_get_by_query__max_items_and_count__raises(self):
        with self.assertRaises(Exception) as e:
            self.dynamo_client.get_by_query({'hash_col': 'key'}, table_name=self.table_name, max_items=3,
                                                           return_count=True)
        expected_msg = "DynamoDbCLient.get_by_query does not support `max_items` and `return_count` together"
        self.assertEqual(e.exception.args[0], expected_msg)


    def test_patch__transfers_attrs_to_remove(self):

        keys = {'hash_col': 'a'}
        attributes_to_update = {'some_col': 'b'}
        attributes_to_increment = {'some_counter': 3}
        table_name = 'the_table'
        attributes_to_remove = ['remove_me']

        # using kwargs
        self.dynamo_client.update = Mock()

        self.dynamo_client.patch(keys=keys, attributes_to_update=attributes_to_update,
                                 attributes_to_increment=attributes_to_increment, table_name=table_name,
                                 attributes_to_remove=attributes_to_remove)

        self.dynamo_client.update.assert_called_once_with(keys=keys, attributes_to_update=attributes_to_update,
                                                          attributes_to_increment=attributes_to_increment,
                                                          table_name=table_name,
                                                          attributes_to_remove=attributes_to_remove,
                                                          condition_expression='attribute_exists hash_col')

        # not kwargs
        self.dynamo_client.update = Mock()

        self.dynamo_client.patch(keys, attributes_to_update, attributes_to_increment, table_name, attributes_to_remove)

        self.dynamo_client.update.assert_called_once_with(keys=keys, attributes_to_update=attributes_to_update,
                                                          attributes_to_increment=attributes_to_increment,
                                                          table_name=table_name,
                                                          attributes_to_remove=attributes_to_remove,
                                                          condition_expression='attribute_exists hash_col')


    def test_sleep_db__get_capacity_called(self):
        self.dynamo_client.get_capacity = MagicMock(return_value={'read': 10, 'write': 5})

        self.dynamo_client.sleep_db(last_action_time=datetime.datetime.now(), action='write')
        self.dynamo_client.get_capacity.assert_called_once()


    def test_sleep_db__wrong_action(self):
        self.assertRaises(KeyError, self.dynamo_client.sleep_db, last_action_time=datetime.datetime.now(),
                          action='call')

    @patch.object(time, 'sleep')
    def test_sleep_db__fell_asleep(self, mock_sleep):
        self.dynamo_client.get_capacity = MagicMock(return_value={'read': 10, 'write': 5})
        # Check that went to sleep
        time_between_ms = 100
        last_action_time = datetime.datetime.now() - datetime.timedelta(milliseconds=time_between_ms)
        self.dynamo_client.sleep_db(last_action_time=last_action_time, action='write')
        self.assertEqual(mock_sleep.call_count, 1)
        args, kwargs = mock_sleep.call_args

        # Should sleep around 1 / capacity second minus "time_between_ms" minus code execution time
        self.assertGreater(args[0], 1 / self.dynamo_client.get_capacity()['write'] - time_between_ms - 0.02)
        self.assertLess(args[0], 1 / self.dynamo_client.get_capacity()['write'])


    @patch.object(time, 'sleep')
    def test_sleep_db__(self, mock_sleep):
        self.dynamo_client.get_capacity = MagicMock(return_value={'read': 10, 'write': 5})

        # Shouldn't go to sleep
        last_action_time = datetime.datetime.now() - datetime.timedelta(milliseconds=900)
        self.dynamo_client.sleep_db(last_action_time=last_action_time, action='write')
        # Sleep function should not be called
        self.assertEqual(mock_sleep.call_count, 0)


if __name__ == '__main__':
    unittest.main()
