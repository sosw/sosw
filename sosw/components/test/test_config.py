import boto3
import csv
import logging
import os
import unittest
from unittest.mock import patch, MagicMock

from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table
from sosw.components.config import SSMConfig, DynamoConfig, ConfigSource


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class SsmTestCase(unittest.TestCase):

    def setUp(self):
        self.ssm_config = SSMConfig(test=True)


    @unittest.skip("Not used in favour of DynamoDB version. Disabled to avoid throttling problems")
    def test_get_credentials_by_prefix(self):
        self.assertEqual(self.ssm_config.get_credentials_by_prefix('autotest_')['ssm_test'], 'hello')
        self.assertEqual(self.ssm_config.get_credentials_by_prefix('autotest')['ssm_test'], 'hello')
        self.assertEqual(len(self.ssm_config.get_credentials_by_prefix('bad_test')), 0)


class FakeDynamo(MagicMock):

    def get_by_query(*args, **kwargs):
        pass  # This is patched later


class DynamoConfigTestCase(unittest.TestCase):
    TEST_CONFIG = {
        'row_mapper':      {
            'env':          'S',
            'config_name':  'S',
            'config_value': 'S'
        },
        'required_fields': ['env', 'config_name', 'config_value'],
        'table_name':      'autotest_config_component'
    }


    def setUp(self):
        config = self.TEST_CONFIG.copy()
        self.dynamo_client = DynamoDbClient(config)
        self.dynamo_config = DynamoConfig(test=True)


    def tearDown(self):
        clean_dynamo_table('autotest_config_component', keys=('env', 'config_name'))


    @unittest.skip("TODO need normal patching")
    def test_get_config__json(self):
        row = {'env': 'production', 'config_name': 'sophie_test', 'config_value': '{"a": 1}'}
        self.dynamo_client.put(row)

        config = self.dynamo_config.get_config('sophie_test', "production")

        self.assertEqual(config, {'a': 1})


    @unittest.skip("TODO need normal patching")
    def test_get_config__str(self):
        def get_by_query(*args, **kwargs):
            return [{'env': 'production', 'config_name': 'sophie_test2', 'config_value': 'some text'}]


        self.dynamo_config.dynamo_client = FakeDynamo
        with patch.object(FakeDynamo, 'get_by_query', new=get_by_query):
            config = self.dynamo_config.get_config('sophie_test2', "production")
            self.assertEqual(config, 'some text')


    def test_get_config__doesnt_exist(self):
        config = self.dynamo_config.get_config('sophie_test', "production")
        self.assertEqual(config, {})


    def test_get_credentials_by_prefix(self):
        SAMPLES = [
            {'env': 'dev', 'config_name': 'testing_zz1', 'config_value': '{"a": 1}'},
            {'env': 'dev', 'config_name': 'testing_zz2', 'config_value': 'zz2_value'},
            {'env': 'dev', 'config_name': 'dont_get_this', 'config_value': '{"b": 2}'},
            {'env': 'dev', 'config_name': 'testingab2', 'config_value': 'some text'}
        ]

        for row in SAMPLES:
            self.dynamo_client.put(row)

        result = self.dynamo_config.get_credentials_by_prefix('testing')

        self.assertEqual(len(result), 2)
        self.assertIn('zz1', result)
        self.assertEqual(result['zz2'], 'zz2_value')


    def test_update_config(self):
        KEY = 'testing_update_method'
        VALUE = 'exists'
        self.dynamo_config.update_config(name=KEY, val=VALUE)
        result = self.dynamo_client.get_by_query(keys={'env': 'dev', 'config_name': KEY})
        self.assertEqual({'env': 'dev', 'config_name': KEY, 'config_value': VALUE}, result[0])


class ConfigTestCase(unittest.TestCase):

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
