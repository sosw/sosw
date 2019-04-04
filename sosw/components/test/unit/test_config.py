import boto3
import csv
import logging
import os
import unittest
from unittest.mock import patch, MagicMock

from sosw.components.config import ConfigSource


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class FakeDynamo:

    def get_by_query(*args, **kwargs):
        pass  # This is patched later


class Config_UnitTestCase(unittest.TestCase):

    def setUp(self):
        with patch('sosw.components.config.DynamoConfig') as patch_dynamo:
            with patch('sosw.components.config.SSMConfig') as patch_ssm:
                self.config_source = ConfigSource(test=True)
                self.patched_ssm = patch_ssm
                self.patched_dynamo = patch_dynamo


    def test_get_config(self):
        # checks it calls dynamo config
        self.config_source.get_config('something')
        self.config_source.default_source.get_config.assert_called_once_with('something')


    def test_update_config(self):
        # checks it calls dynamo config
        self.config_source.update_config('name', 'value')
        self.config_source.default_source.update_config.assert_called_once_with('name', 'value')


    def test_default_sources(self):
        config_source = ConfigSource(test=True)

        self.assertFalse(hasattr(config_source, 'ssm_config'))
        self.assertTrue(hasattr(config_source, 'dynamo_config'))
        self.assertEqual(config_source.default_source, getattr(config_source, 'dynamo_config'))


    def test_custom_client(self):
        # checks it calls dynamo config
        config_source = ConfigSource(test=True, sources='SSM')

        self.assertTrue(hasattr(config_source, 'ssm_config'))
        self.assertFalse(hasattr(config_source, 'dynamo_config'))


    def test_custom_client__multiple(self):
        # checks it calls dynamo config
        config_source = ConfigSource(test=True, sources='SSM, Dynamo')

        self.assertTrue(hasattr(config_source, 'ssm_config'))
        self.assertTrue(hasattr(config_source, 'dynamo_config'))
        self.assertEqual(config_source.default_source, getattr(config_source, 'ssm_config'))


if __name__ == '__main__':
    unittest.main()
