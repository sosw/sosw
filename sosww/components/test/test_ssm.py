import boto3
import logging
import os
import time
import unittest

from random import randint
from ..ssm import *


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"


class ssm_TestCase(unittest.TestCase):
    name = None


    @classmethod
    def setUpClass(cls):
        cls.ssm = boto3.client('ssm')


    def setUp(self):
        self.name = str(randint(1000, 9999))
        self.full_name = f"autotest_{self.name}"
        self.ssm.put_parameter(
                Name=self.full_name,
                Value='test_value',
                Type='SecureString',
                Overwrite=False
        )

        self.ssm.add_tags_to_resource(
                ResourceType='Parameter',
                ResourceId=self.full_name,
                Tags=[
                    {
                        'Key':   'Environment',
                        'Value': 'dev'
                    },
                ]
        )

        time.sleep(1)


    def tearDown(self):
        self.ssm.delete_parameter(Name=self.full_name)
        self.name = self.full_name = None


    def test_get_credentials_by_prefix(self):
        self.assertEqual(get_credentials_by_prefix('autotest_')[self.name], 'test_value')
        self.assertEqual(get_credentials_by_prefix('autotest')[self.name], 'test_value')
        self.assertEqual(len(get_credentials_by_prefix('bad_test')), 0)


if __name__ == '__main__':
    unittest.main()
