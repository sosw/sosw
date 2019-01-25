import logging
import unittest
import os
import boto3
from ..tasks_api_client_for_workers import *

logger = logging.getLogger()

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

TASK_ID = 'test_task_id'
CREATED_MS = '1'
TABLE_NAME = 'autotest_abs_tasks_running'
TEST_ITEM = {
            'created_ms': CREATED_MS,
            'task_id': TASK_ID
            }

TEST_ITEM_DICT = {
            'created_ms': {'N': CREATED_MS},
            'task_id': {'S': TASK_ID}
            }


class tasks_api_client_for_workers_TestCase(unittest.TestCase):

    def setUp(self):
        """ Setting up an item in dynamo DB """

        self.client = boto3.client('dynamodb')
        response = self.client.put_item(TableName=TABLE_NAME, Item=TEST_ITEM_DICT)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Item created successfully.")
        else:
            logger.info("Item creation has failed.")

    def tearDown(self):
        """ Cleaning up the created item from dynamo DB """

        response = self.client.delete_item(TableName=TABLE_NAME, Key=TEST_ITEM_DICT)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Item deleted successfully.")
        else:
            logger.info("Item deletion has failed.")

    def test_close_task(self):
        """ Testing the 'close_task()' function """

        close_task(TEST_ITEM)
        result = self.client.get_item(TableName=TABLE_NAME, Key=TEST_ITEM_DICT)['Item']
        self.assertEqual(result['task_id'], {'S': TASK_ID}, 'TASK_ID assertion failed.')
        self.assertEqual(result['created_ms'], {'N': CREATED_MS}, 'CREATED_MS assertion failed.')


    def test_validation(self):
        item = {'created_ms': {'N': 'abc'}, 'task_id': {'S': 'foo'}}
        self.assertRaises(AssertionError, close_task, item, "Hash Key should be validated for numeric")


if __name__ == '__main__':
    unittest.main()
