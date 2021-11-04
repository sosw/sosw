import boto3
import json
import logging
import time
import unittest
import uuid
import os

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from botocore.exceptions import ClientError
from sosw.components.config import SecretsManager

TEST_SUFFIX = None
RETRIES = 3
TIMEOUT = 10


def _get_autotest_secret_suffix():
    """ Get unique suffix for autotest secrets for this run of TestCase """

    global TEST_SUFFIX

    if TEST_SUFFIX is None:
        TEST_SUFFIX = str(uuid.uuid4())[:18]

    return TEST_SUFFIX


class SecretsManagerIntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = {
    }

    TEST_SECRETS = []
    test_numbers = ['first', 'second', 'third']

    for test_num in test_numbers:
        TEST_SECRETS.append(
        {
            'Name': f'autotest_{test_num}_secret_{_get_autotest_secret_suffix()}',
            'SecretString': json.dumps({'username': f'test_{_get_autotest_secret_suffix()}'}),
            'Tags': [{'Key': 'test_env', 'Value': f'test_{test_num}_{_get_autotest_secret_suffix()}'}]
        })

    client = None

    def _get_boto3_client(self) -> boto3.client:

        if not self.client:
            self.client = boto3.client('secretsmanager')

        return self.client


    @classmethod
    def setUpClass(cls) -> None:

        client = boto3.client('secretsmanager')

        for test in cls.TEST_SECRETS:
            client.create_secret(**test)


    @classmethod
    def tearDownClass(cls) -> None:

        client = boto3.client('secretsmanager')
        for test in cls.TEST_SECRETS:
            client.delete_secret(SecretId=test['Name'])


    def setUp(self):

        self.secretsmanager_obj = SecretsManager()


    def tearDown(self):
        pass


    def waiter(self, name):
        attempts = 0
        while attempts < 5:
            try:
                self.client.describe_secret(SecretId=name)
                break

            except Exception:
                time.sleep(0.5)
                attempts += 1

    def test_get_empty_secrets_credentials(self):
        empty_keys = {'type': 'name', 'value': f'{self.TEST_SECRETS[0]["Tags"][0]["Value"]}'}
        result = self.secretsmanager_obj.get_secrets_credentials(**empty_keys)
        print(f'empty_keys {empty_keys}')
        self.assertEqual(result, {})


    def test_get_secrets_credentials_by_name(self):
        for test in self.TEST_SECRETS:
            self.waiter(test['Name'])
            by_name = {'type': 'name', 'value': test['Name']}
            result = self.secretsmanager_obj.get_secrets_credentials(**by_name)
            expected_result = {test['Name']: test['SecretString']}
            self.assertEqual(result, expected_result)

    def test_get_secrets_credentials_by_tag(self):
        for test in self.TEST_SECRETS:
            self.waiter(test['Name'])
            by_name = {'type': 'tag', 'value': test['Tags'][0]['Value']}
            result = self.secretsmanager_obj.get_secrets_credentials(**by_name)
            expected_result = {test['Name']: test['SecretString']}
            self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
