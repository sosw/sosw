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
#
# class tasks_api_client_for_workers_TestCase(unittest.TestCase):
#
#     def setUp(self):
#         """ Setting up an item in dynamo DB """
#
#         self.patcher = patch("boto3.client")
#         self.boto3_client_patch = self.patcher.start()
#
#
#     def tearDown(self):
#         self.patcher.stop()
#
#
#     @patch('os.environ.get')
#     def test_close_task__laborer_id_from_end(self, env_mock):
#         """ Careful! Once we patch os.environ - we are no longer in STAGE=test! :) """
#
#         env_mock.return_value = LABOURER_ID
#         mock_client = MagicMock()
#         self.boto3_client_patch.return_value = mock_client
#
#         close_task({'task_id': TASK_ID})
#
#         mock_client.update_item.assert_called_once()
#
#         args, kwargs = mock_client.update_item.call_args
#         # You may do some validation here.
#
#
# if __name__ == '__main__':
#     unittest.main()
