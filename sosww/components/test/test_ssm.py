import boto3
import datetime
import logging
import os
import time
import unittest

from random import randint
from unittest import mock
from unittest.mock import Mock, MagicMock
from sosww.components.ssm import *


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"


class ssm_IntegrationTestCase(unittest.TestCase):
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


class ssm_UnitTestCase(unittest.TestCase):


    @mock.patch("boto3.client")
    def test_get_config(self, mock_boto_client):

        client = MagicMock()
        client.get_parameters = Mock(return_value={'Parameters': [{'Name': 'autotest_other',
           'Type': 'String',
           'Value': '{\r\n  "cars": 2,\r\n  "cats": 3\r\n}',
           'Version': 37,
           'LastModifiedDate': datetime.datetime(2018, 1, 1, 1, 1, 1),
           'ARN': 'arn:aws:ssm:us-west-2:737060422660:parameter/autotest_other'}],
         'InvalidParameters': [],
         'ResponseMetadata': {'RequestId': 'ab9ac1ee-67db-4f40-9f7d-b90fe794f23f',
          'HTTPStatusCode': 200,
          'HTTPHeaders': {'x-amzn-requestid': 'ab9ac1ee-67db-4f40-9f7d-b90fe794f23f',
           'content-type': 'application/x-amz-json-1.1',
           'content-length': '250',
           'date': 'Fri, 09 Nov 2018 07:27:05 GMT'},
          'RetryAttempts': 0}})

        mock_boto_client.return_value = client

        self.assertEqual(get_config('test')['cars'], 2)
        self.assertEqual(get_config('test')['cats'], 3)
        self.assertIsNone(get_config('test').get('not_existing'))


if __name__ == '__main__':
    unittest.main()
