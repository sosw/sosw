import boto3
import os
import random
import types
import unittest

from unittest.mock import MagicMock, patch

from sosw.scheduler import Scheduler, global_vars
from sosw.labourer import Labourer
from sosw.test.variables import TEST_SCHEDULER_CONFIG
from sosw.test.helpers_test import line_count

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCHEDULER_CONFIG

    @classmethod
    def setUpClass(cls):
        cls.TEST_CONFIG['init_clients'] = ['S3', ]

        cls.clean_bucket()


    @staticmethod
    def clean_bucket():
        """ Clean S3 bucket"""

        s3 = boto3.resource('s3')
        bucket = s3.Bucket('autotest-bucket')
        bucket.objects.all().delete()


    def exists_in_s3(self, key):
        try:
            self.s3_client.get_object(Bucket='autotest-bucket', Key=key)
            return True
        except self.s3_client.exceptions.ClientError:
            return False


    def put_file(self, local=None, key=None, only_remote=False):
        self.make_local_file('Liat')

        self.s3_client.upload_file(Filename=local or self.scheduler.local_queue_file,
                                   Bucket='autotest-bucket',
                                   Key=key or self.scheduler.remote_queue_file)

        if only_remote:
            try:
                os.remove(local or self.scheduler.local_queue_file)
            except Exception:
                pass


    def make_local_file(self, girl_name="Athena", fname=None, rows=10):

        with open(fname or self.scheduler.local_queue_file, 'w') as f:
            for i in range(rows):
                f.write(f"hello {girl_name} {i} {random.randint(0,99)}\n")


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        lambda_context = types.SimpleNamespace()
        lambda_context.aws_request_id = 'AWS_REQ_ID'
        lambda_context.invoked_function_arn = 'arn:aws:lambda:us-west-2:000000000000:function:some_function'
        lambda_context.get_remaining_time_in_millis = MagicMock(return_value=300000)  # 5 minutes
        global_vars.lambda_context = lambda_context
        self.custom_lambda_context = global_vars.lambda_context  # This is to access from tests.

        self.scheduler = Scheduler(self.custom_config)

        self.s3_client = boto3.client('s3')


    def tearDown(self):
        self.patcher.stop()
        self.clean_bucket()

        try:
            os.remove(self.scheduler.local_queue_file)
        except Exception:
            pass

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_get_and_lock_queue_file(self):
        self.put_file(only_remote=True)

        # Check old artifacts
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))

        r = self.scheduler.get_and_lock_queue_file()

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

        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))


    def test_upload_and_unlock_queue_file__handles_existing_locked(self):

        self.put_file(key=self.scheduler.remote_queue_locked_file)

        # Check old artifacts
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_file))

        self.make_local_file('Nora')

        self.scheduler.upload_and_unlock_queue_file()

        self.assertFalse(self.exists_in_s3(self.scheduler.remote_queue_locked_file))
        self.assertTrue(self.exists_in_s3(self.scheduler.remote_queue_file))
