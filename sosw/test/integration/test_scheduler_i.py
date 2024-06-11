import asyncio
import logging

import boto3
import os
import random
import time
import types
import unittest

from unittest.mock import MagicMock, patch

from sosw.scheduler import Scheduler, global_vars
from sosw.labourer import Labourer
from sosw.test.variables import TEST_SCHEDULER_CONFIG
from sosw.test.helpers_test import line_count
from sosw.test.helpers_test_dynamo_db import AutotestDdbManager, autotest_dynamo_db_tasks_setup

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_IntegrationTestCase(unittest.TestCase):
    AWS_ACCOUNT = None
    BUCKET_NAME = None
    TEST_CONFIG = TEST_SCHEDULER_CONFIG
    autotest_ddbm = None


    @classmethod
    def setUpClass(cls):
        cls.TEST_CONFIG['init_clients'] = ['S3', ]

        cls.AWS_ACCOUNT = boto3.client('sts').get_caller_identity().get('Account')
        cls.BUCKET_NAME = f'autotest-sosw-s3-{cls.AWS_ACCOUNT}'
        cls.clean_bucket(cls.BUCKET_NAME)
        tables = [autotest_dynamo_db_tasks_setup]
        cls.autotest_ddbm = AutotestDdbManager(tables)


    @classmethod
    def tearDownClass(cls) -> None:
        cls.clean_bucket(cls.BUCKET_NAME)
        asyncio.run(cls.autotest_ddbm.drop_ddbs())


    @staticmethod
    def clean_bucket(name):
        """ Clean S3 bucket"""
        logging.info("Cleaning bucket %s", name)
        client = boto3.client('s3')

        response = client.list_objects_v2(Bucket=name)
        try:
            files = [x['Key'] for x in response['Contents']]
        except KeyError:
            logging.info("No files to clean")
            return
        logging.info("Deleting files: %s", files)

        R = client.delete_objects(
            Bucket=name,
            Delete={'Objects': [{'Key': fname} for fname in files]}
        )
        time.sleep(6)
        response = client.list_objects_v2(Bucket=name)
        print("AFTER CLEANING")
        print(response)


    def exists_in_s3(self, key):
        try:
            self.s3_client.get_object(Bucket=self.BUCKET_NAME, Key=key)
            return True
        except self.s3_client.exceptions.ClientError:
            return False


    def put_file(self, local=None, key=None, only_remote=False):
        self.make_local_file('Liat')

        self.s3_client.upload_file(Filename=local or self.scheduler.local_queue_file,
                                   Bucket=self.BUCKET_NAME,
                                   Key=key or self.scheduler.remote_queue_file)
        time.sleep(0.3)

        if only_remote:
            try:
                os.remove(local or self.scheduler.local_queue_file)
            except Exception:
                pass


    def make_local_file(self, girl_name="Athena", fname=None, rows=10):

        with open(fname or self.scheduler.local_queue_file, 'w') as f:
            for i in range(rows):
                f.write(f"hello {girl_name} {i} {random.randint(0, 99)}\n")


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.get_config_patch.return_value = {}

        self.custom_config = self.TEST_CONFIG.copy()
        self.custom_config['queue_bucket'] = self.BUCKET_NAME
        lambda_context = types.SimpleNamespace()
        lambda_context.aws_request_id = 'AWS_REQ_ID'
        lambda_context.invoked_function_arn = 'arn:aws:lambda:us-west-2:000000000000:function:some_function'
        lambda_context.get_remaining_time_in_millis = MagicMock(return_value=300000)  # 5 minutes
        global_vars.lambda_context = lambda_context
        self.custom_lambda_context = global_vars.lambda_context  # This is to access from tests.

        self.scheduler = Scheduler(self.custom_config)
        self.scheduler.task_client = MagicMock()
        self.s3_client = boto3.client('s3')


    def tearDown(self):
        self.patcher.stop()
        self.clean_bucket(self.BUCKET_NAME)

        try:
            os.remove(self.scheduler.local_queue_file)
        except Exception:
            pass

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass

        asyncio.run(self.autotest_ddbm.clean_ddbs())


    def test_get_and_lock_queue_file(self):
        self.put_file(only_remote=True)

        # Check old artifacts
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))

        r = self.scheduler.get_and_lock_queue_file()
        time.sleep(0.3)

        self.assertEqual(r, self.scheduler.local_queue_file)

        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_file))

        number_of_lines = line_count(self.scheduler.local_queue_file)
        # print(f"Number of lines: {number_of_lines}")
        self.assertTrue(number_of_lines, 10)


    def test_upload_and_unlock_queue_file(self):

        # Check old artifacts
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_file))

        self.make_local_file('Demida')

        self.scheduler.upload_and_unlock_queue_file()
        time.sleep(0.3)

        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))


    def test_upload_and_unlock_queue_file__handles_existing_locked(self):

        self.put_file(key=self.scheduler.remote_queue_locked_file)

        # Check old artifacts
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_file))

        self.make_local_file('Nora')

        self.scheduler.upload_and_unlock_queue_file()
        time.sleep(0.3)

        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))
