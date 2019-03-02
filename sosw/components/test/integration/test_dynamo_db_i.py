import boto3
import logging
import time
import unittest
import os
from collections import defaultdict


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table


class dynamodb_client_IntegrationTestCase(unittest.TestCase):
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


    @classmethod
    def setUpClass(cls):
        clean_dynamo_table()


    def setUp(self):
        self.HASH_KEY = ('hash_col', 'S')
        self.RANGE_KEY = ('range_col', 'N')
        self.KEYS = ('hash_col', 'range_col')
        self.table_name = 'autotest_dynamo_db'
        self.dynamo_client = DynamoDbClient(config=self.TEST_CONFIG)


    def tearDown(self):
        clean_dynamo_table(self.table_name, self.KEYS)


    def test_put(self):
        row = {'hash_col': 'cat', 'range_col': '123'}

        client = boto3.client('dynamodb')

        client.delete_item(TableName=self.table_name,
                           Key={
                               'hash_col':  {'S': str(row['hash_col'])},
                               'range_col': {'N': str(row['range_col'])},
                           })

        self.dynamo_client.put(row, self.table_name)

        result = client.scan(TableName=self.table_name,
                             FilterExpression="hash_col = :hash_col AND range_col = :range_col",
                             ExpressionAttributeValues={
                                 ':hash_col':  {'S': row['hash_col']},
                                 ':range_col': {'N': str(row['range_col'])}
                             }
                             )

        items = result['Items']

        self.assertTrue(len(items) > 0)


    def test_update__updates(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no', 'other_col': 'foo'}
        attributes_to_update = {'some_col': 'yes', 'new_col': 'yup'}

        self.dynamo_client.put(row, self.table_name)

        client = boto3.client('dynamodb')

        # First check that the row we are trying to update is PUT correctly.
        initial_row = client.get_item(
                Key={
                    'hash_col':  {'S': row['hash_col']},
                    'range_col': {'N': str(row['range_col'])}
                },
                TableName=self.table_name,
        )['Item']

        initial_row = self.dynamo_client.dynamo_to_dict(initial_row)

        self.assertIsNotNone(initial_row)
        self.assertEqual(initial_row['some_col'], 'no')
        self.assertEqual(initial_row['other_col'], 'foo')

        self.dynamo_client.update(keys, attributes_to_update, None, self.table_name)

        updated_row = client.get_item(
                Key={
                    'hash_col':  {'S': row['hash_col']},
                    'range_col': {'N': str(row['range_col'])}
                },
                TableName=self.table_name,
        )['Item']

        updated_row = self.dynamo_client.dynamo_to_dict(updated_row)

        self.assertIsNotNone(updated_row)
        self.assertEqual(updated_row['some_col'], 'yes'), "Updated field not really updated"
        self.assertEqual(updated_row['new_col'], 'yup'), "New field was not created"
        self.assertEqual(updated_row['other_col'], 'foo'), "This field should be preserved, update() damaged it"


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


    def test_update__increment_2(self):
        keys = {'hash_col': 'cat', 'range_col': '123'}
        row = {'hash_col': 'cat', 'range_col': '123', 'some_col': 'no', 'some_counter': 10}
        attributes_to_increment = {'some_counter': 5}

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
        self.assertEqual(updated_row['some_counter'], 15)


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


    def test_get_by_query__comparison_between(self):
        # Put sample data
        x = [self.dynamo_client.put({'hash_col': 'cat', 'range_col': x}, self.table_name) for x in range(10)]

        keys = {'hash_col': 'cat', 'st_between_range_col': '3', 'en_between_range_col': '6'}
        result = self.dynamo_client.get_by_query(keys=keys, comparisons={'range_col': 'between'})
        # print(result)
        self.assertTrue(all(x['range_col'] in range(3, 7) for x in result))

        result = self.dynamo_client.get_by_query(keys=keys)
        # print(result)
        self.assertTrue(all(x['range_col'] in range(3, 7) for x in result)), "Failed if unspecified comparison. " \
                                                                             "Should be automatic for :st_between_..."


    def test_get_by_query__filter_expression(self):
        """
        This _integration_ test runs multiple checks with same sample data for several comparators.
        Have a look at the manual if required:
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
        """

        # Put sample data
        [self.dynamo_client.put({'hash_col': 'cat', 'range_col': x}, self.table_name) for x in range(3)]
        [self.dynamo_client.put({'hash_col': 'cat', 'range_col': x, 'mark': 1}, self.table_name) for x in range(3,6)]
        self.dynamo_client.put({'hash_col': 'cat', 'range_col': 6, 'mark': 0}, self.table_name)
        self.dynamo_client.put({'hash_col': 'cat', 'range_col': 7, 'mark': 'a'}, self.table_name)

        # Condition by range_col will return five rows out of six: 0 - 4
        # Filter expression neggs the first three rows because they don't have `mark = 1`.
        keys = {'hash_col': 'cat', 'range_col': 4}
        result = self.dynamo_client.get_by_query(keys=keys, comparisons={'range_col': '<='},
                                                 strict=False, filter_expression='mark = 1')
        # print(result)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {'hash_col': 'cat', 'range_col': 3, 'mark': 1})
        self.assertEqual(result[1], {'hash_col': 'cat', 'range_col': 4, 'mark': 1})

        # In the same test we check also some comparator _functions_.
        result = self.dynamo_client.get_by_query(keys=keys, comparisons={'range_col': '<='},
                                                 strict=False, filter_expression='attribute_exists mark')
        # print(result)
        self.assertEqual(len(result), 2)
        self.assertEqual([x['range_col'] for x in result], list(range(3,5)))

        self.assertEqual(result[0], {'hash_col': 'cat', 'range_col': 3, 'mark': 1})
        self.assertEqual(result[1], {'hash_col': 'cat', 'range_col': 4, 'mark': 1})


        result = self.dynamo_client.get_by_query(keys=keys, comparisons={'range_col': '<='},
                                                 strict=False, filter_expression='attribute_not_exists mark')
        # print(result)
        self.assertEqual(len(result), 3)
        self.assertEqual([x['range_col'] for x in result], list(range(3)))



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

        self.dynamo_client = DynamoDbClient(config=config)

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


    @unittest.skip("This test takes too long time to run every time. It passes.")
    def test_get_by_query__max_items(self):

        for x in range(1000, 2000):
            row = {'hash_col': f"key", 'range_col': x}
            self.dynamo_client.put(row, self.table_name)
            time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.

        st = time.perf_counter()
        result = self.dynamo_client.get_by_query({'hash_col': 'key'}, table_name=self.table_name, max_items=3)
        bm = time.perf_counter() - st
        # print(f"Benchmark: {bm}")

        self.assertEqual(len(result), 3)
        self.assertLess(bm, 0.1)

        st = time.perf_counter()
        result = self.dynamo_client.get_by_query({'hash_col': 'key'}, table_name=self.table_name, max_items=499)
        bm = time.perf_counter() - st
        # print(f"Benchmark: {bm}")
        self.assertLess(bm, 0.1)

        self.assertEqual(len(result), 499)


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

        self.assertEqual(len(result), 2)

        self.assertIn(rows[0], result)
        self.assertIn(rows[2], result)


if __name__ == '__main__':
    unittest.main()
