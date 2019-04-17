# import logging
# import unittest
# import os
# import boto3
#
# from unittest.mock import patch, MagicMock
#
# from sosw.components.tasks_api_client_for_workers import *
#
#
# logger = logging.getLogger()
#
# os.environ["STAGE"] = "test"
# os.environ["autotest"] = "True"
#
# TASK_ID = 'test_task_id'
# LABOURER_ID = 'some_lambda'
# TABLE_NAME = 'autotest_sosw_tasks'
#
# TEST_ITEM = {
#     'task_id':     TASK_ID,
#     'labourer_id': LABOURER_ID
# }
#
# TEST_ITEM_DICT = {
#     'task_id':     {'S': TASK_ID}
# }
#
#
# class tasks_api_client_for_workers_TestCase(unittest.TestCase):
#
#     def setUp(self):
#         """ Setting up an item in dynamo DB """
#
#         self.client = boto3.client('dynamodb')
#         response = self.client.put_item(TableName=TABLE_NAME, Item=TEST_ITEM_DICT)
#         if response['ResponseMetadata']['HTTPStatusCode'] == 200:
#             logger.info("Item created successfully.")
#         else:
#             raise RuntimeError("Item creation has failed.")
#
#
#     def tearDown(self):
#         """ Cleaning up the created item from dynamo DB """
#
#         response = self.client.delete_item(TableName=TABLE_NAME, Key=TEST_ITEM_DICT)
#         if response['ResponseMetadata']['HTTPStatusCode'] == 200:
#             logger.info("Item deleted successfully.")
#         else:
#             raise RuntimeError("Item deletion has failed.")
#
#
#     def test_close_task(self):
#         """ Testing the 'close_task()' function """
#         with patch('time.time') as t:
#             t.return_value = 9500
#             close_task(TEST_ITEM)
#
#         result = self.client.get_item(TableName=TABLE_NAME, Key=TEST_ITEM_DICT)['Item']
#         self.assertEqual(result['task_id'], {'S': TASK_ID})
#         self.assertEqual(result['completed_at'], {'N': '9500'})
#
#
# if __name__ == '__main__':
#     unittest.main()
