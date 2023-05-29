import logging
import unittest
import os

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.config import SecretsManager


class secretsmanager_client_UnitTestCase(unittest.TestCase):


    def setUp(self):
        self.secretsmanager_obj = SecretsManager()

    def tearDown(self):
        pass

    def test_key_error(self):
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'a': 'b'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'tag': 'v', 'value': 'test'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'type': '', 'value': 'test'})


if __name__ == '__main__':
    unittest.main()
