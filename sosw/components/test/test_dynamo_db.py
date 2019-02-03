import boto3
import logging
import unittest
import os
from collections import defaultdict


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.helpers import *
from sosw.components.dynamo_db import DynamoDBClient, clean_dynamo_table


class dynamodb_client_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = {
        'row_mapper': {
            'lambda_name': 'S',
            'invocation_id': 'S',
            'en_time': 'N',

            'hash_col': 'S',
            'range_col': 'N',
            'other_col': 'S',
            'new_col': 'S',
            'some_col': 'S',
            'some_counter': 'N'
        },
        'required_fields': ['lambda_name'],
        'table_name': 'autotest_dynamo_db'
    }

    @classmethod
    def setUpClass(cls):
        clean_dynamo_table()


    def setUp(self):
        self.HASH_KEY = ('hash_col', 'S')
        self.RANGE_KEY = ('range_col', 'N')
        self.KEYS = ('hash_col', 'range_col')
        self.table_name = 'autotest_dynamo_db'
        self.dynamo_client = DynamoDBClient(config=self.TEST_CONFIG)


    def tearDown(self):
        clean_dynamo_table(self.table_name, self.KEYS)

    def test_put(self):
        row = {'hash_col': 'cat', 'range_col': '123'}

        client = boto3.client('dynamodb')

        client.delete_item(TableName=self.table_name,
                           Key={
                               'hash_col': {'S': str(row['hash_col'])},
                               'range_col': {'N': str(row['range_col'])},
                           })

        self.dynamo_client.put(row, self.table_name)

        result = client.scan(TableName=self.table_name,
                    FilterExpression="hash_col = :hash_col AND range_col = :range_col",
                    ExpressionAttributeValues={
                        ':hash_col': {'S': row['hash_col']},
                        ':range_col': {'N': str(row['range_col'])}
                    }
                )

        items = result['Items']

        self.assertTrue(len(items) > 0)

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
        dynamo_row = {'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
                      'extra_key': {'N': '42'}}
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row)
        expected = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456}
        self.assertDictEqual(dict_row, expected)

    def test_dynamo_to_dict_no_strict_row_mapper(self):
        dynamo_row = {'lambda_name': {'S': 'test_name'}, 'invocation_id': {'S': 'test_id'}, 'en_time': {'N': '123456'},
                      'extra_key_n': {'N': '42'}, 'extra_key_s': {'S': 'wowie'}}
        dict_row = self.dynamo_client.dynamo_to_dict(dynamo_row, strict=False)
        expected = {'lambda_name': 'test_name', 'invocation_id': 'test_id', 'en_time': 123456, 'extra_key_n': 42,
                    'extra_key_s': 'wowie'}
        self.assertDictEqual(dict_row, expected)


    def test_dynamo_to_dict__dont_json_loads(self):
        config = self.TEST_CONFIG.copy()
        config['dont_json_loads_results'] = True

        self.dynamo_client = DynamoDBClient(config=config)

        dynamo_row = {'hash_col': {'S': 'aaa'}, 'range_col': {'N': '123'}, 'other_col': {'S': '{"how many": 300}'},
                      'duck_quack': {'S': '{"quack": "duck"}'}}
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

        self.dynamo_client = DynamoDBClient(config=config)

        dynamo_row = {'hash_col': {'S': 'aaa'}, 'range_col': {'N': '123'}, 'other_col': {'S': '{"how many": 300}'},
                      'duck_quack': {'S': '{"quack": "duck"}'}}
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


    def test_update__updates(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no'}
        attributes_to_update = {'some_col': 'yes', 'new_col': 'yup'}

        self.dynamo_client.put(row, self.table_name)

        self.dynamo_client.update(keys, attributes_to_update, None, self.table_name)

        client = boto3.client('dynamodb')

        updated_row = client.get_item(
                Key={
                    'hash_col':  {'S': row['hash_col']},
                    'range_col': {'N': str(row['range_col'])}
                },
                TableName=self.table_name,
        )['Item']

        updated_row = self.dynamo_client.dynamo_to_dict(updated_row)

        self.assertIsNotNone(updated_row)
        self.assertEqual(updated_row['some_col'], 'yes')
        self.assertEqual(updated_row['new_col'], 'yup')

    def test_update__increment(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no', 'some_counter': 10}
        attributes_to_increment = {'some_counter': '1'}

        self.dynamo_client.put(row, self.table_name)

        self.dynamo_client.update(keys, {}, attributes_to_increment, self.table_name)

        client = boto3.client('dynamodb')

        updated_row = client.get_item(
                Key={
                    'hash_col':  {'S': row['hash_col']},
                    'range_col': {'N': str(row['range_col'])}
                },
                TableName=self.table_name,
        )['Item']

        updated_row = self.dynamo_client.dynamo_to_dict(updated_row)

        self.assertIsNotNone(updated_row)
        self.assertEqual(updated_row['some_counter'], 11)


    def test_update__increment_no_default(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no'}
        attributes_to_increment = {'some_counter': '3'}

        self.dynamo_client.put(row, self.table_name)

        self.dynamo_client.update(keys, {}, attributes_to_increment, self.table_name)

        client = boto3.client('dynamodb')

        updated_row = client.get_item(
                Key={
                    'hash_col':  {'S': row['hash_col']},
                    'range_col': {'N': str(row['range_col'])}
                },
                TableName=self.table_name,
        )['Item']

        updated_row = self.dynamo_client.dynamo_to_dict(updated_row)

        self.assertIsNotNone(updated_row)
        self.assertEqual(updated_row['some_counter'], 3)


    def test_get_by_query__primary_index(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': 123, 'some_col': 'test'}
        self.dynamo_client.put(row, self.table_name)

        result = self.dynamo_client.get_by_query(keys=keys)

        self.assertEqual(len(result), 1)
        result = result[0]
        for key in row:
            self.assertEqual(row[key], result[key])
        for key in result:
            self.assertEqual(row[key], result[key])

    def test_get_by_query__primary_index__gets_multiple(self):
        row = {'hash_col': 'cat', 'range_col': 123, 'some_col': 'test'}
        self.dynamo_client.put(row, self.table_name)

        row2 = {'hash_col': 'cat', 'range_col': 1234, 'some_col': 'test2'}
        self.dynamo_client.put(row2, self.table_name)

        result = self.dynamo_client.get_by_query(keys={'hash_col': 'cat'})

        self.assertEqual(len(result), 2)

        result1 = [x for x in result if x['range_col'] == row['range_col']][0]
        result2 = [x for x in result if x['range_col'] == row2['range_col']][0]

        for key in row:
            self.assertEqual(row[key], result1[key])
        for key in result1:
            self.assertEqual(row[key], result1[key])
        for key in row2:
            self.assertEqual(row2[key], result2[key])
        for key in result2:
            self.assertEqual(row2[key], result2[key])

    def test_get_by_query__secondary_index(self):
        keys = {'hash_col': 'cat', 'other_col': 'abc123'}
        row = {'hash_col': 'cat', 'range_col': 123, 'other_col': 'abc123'}
        self.dynamo_client.put(row, self.table_name)

        result = self.dynamo_client.get_by_query(keys=keys, index_name='autotest_index')

        self.assertEqual(len(result), 1)
        result = result[0]
        for key in row:
            self.assertEqual(row[key], result[key])
        for key in result:
            self.assertEqual(row[key], result[key])


    def test_get_by_query__comparison(self):
        keys = {'hash_col': 'cat', 'range_col': '300'}
        row1 = {'hash_col': 'cat', 'range_col': 123, 'other_col': 'abc123'}
        row2 = {'hash_col': 'cat', 'range_col': 456, 'other_col': 'abc123'}
        self.dynamo_client.put(row1, self.table_name)
        self.dynamo_client.put(row2, self.table_name)

        result = self.dynamo_client.get_by_query(keys=keys, comparisons={'range_col': '<='})

        self.assertEqual(len(result), 1)

        result = result[0]
        self.assertEqual(result, row1)


    def test_get_by_query__comparison_begins_with(self):
        self.table_name = 'autotest_config'  # This table has a string range key
        self.HASH_KEY = ('env', 'S')
        self.RANGE_KEY = ('config_name', 'S')
        self.KEYS = ('env', 'config_name')
        config = {
            'row_mapper':      {
                'env':          'S',
                'config_name':  'S',
                'config_value': 'S'
            },
            'required_fields': ['env', 'config_name', 'config_value'],
            'table_name':      'autotest_config'
        }

        self.dynamo_client = DynamoDBClient(config=config)

        row1 = {'env': 'cat', 'config_name': 'testzing', 'config_value': 'abc123'}
        row2 = {'env': 'cat', 'config_name': 'dont_get_this', 'config_value': 'abc123'}
        row3 = {'env': 'cat', 'config_name': 'testzer', 'config_value': 'abc124'}
        self.dynamo_client.put(row1, self.table_name)
        self.dynamo_client.put(row2, self.table_name)
        self.dynamo_client.put(row3, self.table_name)

        keys = {'env': 'cat', 'config_name': 'testz'}
        result = self.dynamo_client.get_by_query(keys=keys, table_name=self.table_name,
                                                 comparisons={'config_name': 'begins_with'})

        self.assertEqual(len(result), 2)

        self.assertTrue(row1 in result)
        self.assertTrue(row3 in result)


    def test_get_by_scan__all(self):
        rows = [
            {'hash_col': 'cat1', 'range_col': 121, 'some_col': 'test1'},
            {'hash_col': 'cat2', 'range_col': 122, 'some_col': 'test2'},
            {'hash_col': 'cat3', 'range_col': 123, 'some_col': 'test3'}
        ]
        for x in rows:
            self.dynamo_client.put(x, self.table_name)

        result = self.dynamo_client.get_by_scan()

        self.assertEqual(len(result), 3)

        for r in rows:
            assert r in result, f"row not in result from dynamo scan: {r}"


    def test_get_by_scan__with_filter(self):
        rows = [
            {'hash_col': 'cat1', 'range_col': 121, 'some_col': 'test1'},
            {'hash_col': 'cat1', 'range_col': 122, 'some_col': 'test2'},
            {'hash_col': 'cat2', 'range_col': 122, 'some_col': 'test2'},
        ]
        for x in rows:
            self.dynamo_client.put(x, self.table_name)

        filter = {'some_col': 'test2'}

        result = self.dynamo_client.get_by_scan(attrs=filter)

        self.assertEqual(len(result), 2)

        for r in rows[1:]:
            assert r in result, f"row not in result from dynamo scan: {r}"


    def test_batch_get_items(self):
        rows = [
            {'hash_col': 'cat1', 'range_col': 121, 'some_col': 'test1'},
            {'hash_col': 'cat1', 'range_col': 122, 'some_col': 'test2'},
            {'hash_col': 'cat2', 'range_col': 122, 'some_col': 'test2'},
        ]
        for x in rows:
            self.dynamo_client.put(x, self.table_name)

        keys_list_query = [
            {'hash_col': 'cat1', 'range_col': 121},
            {'hash_col': 'doesnt_exist', 'range_col': 40},
            {'hash_col': 'cat2', 'range_col': 122},
        ]

        result = self.dynamo_client.batch_get_items_one_table(keys_list_query)

        self.assertEquals(len(result), 2)

        self.assertIn(rows[0], result)
        self.assertIn(rows[2], result)


if __name__ == '__main__':
    unittest.main()
