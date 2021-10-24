import boto3
import json
import logging
import time
import random
import unittest
import uuid
import os


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.config import SecretsManager
from sosw.components.helpers import chunks

TEST_SUFFIX = None

def _get_autotest_secret_suffix():
    """ Get unique suffix for autotest secrets for this run of TestCase """

    global TEST_SUFFIX

    if TEST_SUFFIX is None:
        TEST_SUFFIX = str(uuid.uuid4())[:18]

    return TEST_SUFFIX


class SecretsManagerIntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = {
    }

    TEST_SECRETS = [
        {
            'Name': f'autotest_simple_{_get_autotest_secret_suffix()}',
            'SecretString': json.dumps({"username": "user_value"}),
        },
    ]


    client = None


    def _get_boto3_client(self) -> boto3.client:

        if not self.client:
            self.client = boto3.client('secretsmanager')

        return self.client


    @classmethod
    def setUpClass(cls) -> None:

        client = boto3.client('secretsmanager')
        for test in cls.TEST_SECRETS:
            response = client.create_secret(**test)


    @classmethod
    def tearDownClass(cls) -> None:

        client = boto3.client('secretsmanager')
        for test in cls.TEST_SECRETS:
            client.delete_secret(SecretId=test['Name'])


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_sleep(self):
        time.sleep(10)

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
