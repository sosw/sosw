import logging
import os
import unittest
from unittest.mock import patch, MagicMock

from sosw.components.config import ConfigSource, DynamoConfig, SSMConfig


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

# Patch of the environment simulating a real production run (not autotest).
PROD_ENV = {'STAGE': 'production', 'autotest': 'False'}


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


    def test_get_credentials_by_prefix(self):
        # checks it calls dynamo config
        self.config_source.get_credentials_by_prefix('db')
        self.config_source.default_source.get_credentials_by_prefix.assert_called_once_with('db')


    def test_get_secrets_credentials(self):
        # checks it calls the SecretsManager client
        self.config_source.secrets_manager_class = MagicMock()

        self.config_source.get_secrets_credentials(type='name', value='db_password')
        self.config_source.secrets_manager_class.get_secrets_credentials.assert_called_once_with(
                type='name', value='db_password')


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


    def test_init__sources_of_unsupported_type_raise(self):
        self.assertRaises(ValueError, ConfigSource, test=True, sources=['Dynamo'])


    def test_init__unsupported_source_name_raises(self):
        self.assertRaises(AssertionError, ConfigSource, test=True, sources='Redis')


    def test_init__passes_custom_config_to_source_class(self):
        with patch('sosw.components.config.DynamoConfig') as patched_dynamo:
            ConfigSource(test=True, config={'dynamo_config': {'some_setting': 42}})

        patched_dynamo.assert_called_once_with(config={'some_setting': 42}, test=True)


    def test_init__explicit_test_true_wins_over_env(self):
        with patch.dict(os.environ, PROD_ENV):
            config_source = ConfigSource(test=True)

        self.assertTrue(config_source.test)


    def test_init__explicit_test_false_wins_over_env(self):
        # The module level environment of the tests has STAGE = 'test', but the explicitly provided flag must win.
        self.assertEqual(os.environ.get('STAGE'), 'test')

        config_source = ConfigSource(test=False)
        self.assertFalse(config_source.test)


    def test_init__test_derived_from_env_when_not_provided(self):
        self.assertTrue(ConfigSource().test)

        with patch.dict(os.environ, PROD_ENV):
            self.assertFalse(ConfigSource().test)


class SSMConfig_UnitTestCase(unittest.TestCase):

    def setUp(self):
        self.ssm_config = SSMConfig(test=True)
        self.mock_client = MagicMock()
        self.ssm_config.ssm_client = self.mock_client


    def test_init__test_derived_from_env(self):
        self.assertTrue(SSMConfig().test)

        with patch.dict(os.environ, PROD_ENV):
            self.assertFalse(SSMConfig().test)

        with patch.dict(os.environ, {'STAGE': 'production', 'autotest': 'True'}):
            self.assertTrue(SSMConfig().test)


    def test_get_ssm_client__initializes_once(self):
        ssm_config = SSMConfig(test=True)

        with patch('sosw.components.config.boto3.client') as mock_boto_client:
            first = ssm_config._get_ssm_client()
            second = ssm_config._get_ssm_client()

        self.assertIs(first, mock_boto_client.return_value)
        self.assertIs(second, first)
        mock_boto_client.assert_called_once_with('ssm')


    def test_get_config(self):
        self.mock_client.get_parameters.return_value = {'Parameters': [{'Value': '{"a": 1}'}]}

        self.assertEqual(self.ssm_config.get_config('some_config'), {'a': 1})
        self.mock_client.get_parameters.assert_called_once_with(Names=['some_config'], WithDecryption=True)


    def test_get_config__retries_without_decryption(self):
        self.mock_client.get_parameters.side_effect = [Exception("AccessDenied"),
                                                       {'Parameters': [{'Value': '{"a": 1}'}]}]

        self.assertEqual(self.ssm_config.get_config('some_config'), {'a': 1})
        self.assertEqual(self.mock_client.get_parameters.call_count, 2)
        self.mock_client.get_parameters.assert_called_with(Names=['some_config'], WithDecryption=False)


    def test_get_config__invalid_response_returns_empty_dict(self):
        for response in [{}, {'Parameters': []}, {'Parameters': [{'Value': None}]}]:
            self.mock_client.get_parameters.return_value = response
            self.assertEqual(self.ssm_config.get_config('some_config'), {}, f"Failed for response: {response}")


    def test_update_config__defaults(self):
        self.ssm_config.update_config('some_param', 'some_value')

        self.mock_client.put_parameter.assert_called_once_with(
                Name='some_param', Description='', Value='some_value', Type='String', Overwrite=True)


    def test_update_config__custom_description_and_type(self):
        self.ssm_config.update_config('some_param', 'some_value', description='Some param', param_type='SecureString')

        self.mock_client.put_parameter.assert_called_once_with(
                Name='some_param', Description='Some param', Value='some_value', Type='SecureString', Overwrite=True)


    def test_update_config__invalid_kwargs_fall_back_to_defaults(self):
        self.ssm_config.update_config('some_param', 'some_value', description=42, param_type='MagicString')

        self.mock_client.put_parameter.assert_called_once_with(
                Name='some_param', Description='', Value='some_value', Type='String', Overwrite=True)


    def test_call_boto_with_pagination__native_paginator(self):
        self.mock_client.can_paginate.return_value = True
        paginator = self.mock_client.get_paginator.return_value
        paginator.paginate.return_value = iter([{'Parameters': ['a']}, {'Parameters': ['b']}])

        result = self.ssm_config.call_boto_with_pagination('describe_parameters', ParameterFilters=[])

        self.assertEqual(result, [{'Parameters': ['a']}, {'Parameters': ['b']}])
        self.mock_client.can_paginate.assert_called_once_with('describe_parameters')
        self.mock_client.get_paginator.assert_called_once_with('describe_parameters')
        paginator.paginate.assert_called_once_with(ParameterFilters=[])


    def test_call_boto_with_pagination__manual_pagination(self):
        self.mock_client.can_paginate.return_value = False
        self.mock_client.get_parameter.side_effect = [{'Parameter': 'a'}]

        result = self.ssm_config.call_boto_with_pagination('get_parameter', Name='some_param')

        self.assertEqual(result, [{'Parameter': 'a'}])
        self.mock_client.get_parameter.assert_called_once_with(Name='some_param')


    def test_call_boto_with_pagination__manual_pagination_with_token(self):
        """
        A response carrying a `NextToken` must trigger a follow-up call with the token of the latest page.
        All pages accumulate in the result and the loop terminates on the first page without a token.
        """

        self.mock_client.can_paginate.return_value = False
        self.mock_client.get_parameter.side_effect = [{'Parameter': 'a', 'NextToken': 'token_1'},
                                                      {'Parameter': 'b', 'NextToken': 'token_2'},
                                                      {'Parameter': 'c'}]

        result = self.ssm_config.call_boto_with_pagination('get_parameter', Name='some_param')

        self.assertEqual(result, [{'Parameter': 'a', 'NextToken': 'token_1'},
                                  {'Parameter': 'b', 'NextToken': 'token_2'},
                                  {'Parameter': 'c'}])
        self.assertEqual(self.mock_client.get_parameter.call_count, 3)
        self.mock_client.get_parameter.assert_any_call(Name='some_param', NextToken='token_1')
        self.mock_client.get_parameter.assert_called_with(Name='some_param', NextToken='token_2')


    def test_get_credentials_by_prefix(self):
        describe_response = [{'Parameters': [{'Name': 'db_user', 'Type': 'String'},
                                             {'Name': 'db_pass', 'Type': 'SecureString'}]}]
        get_response = [{'Parameters': [{'Name': 'db_user', 'Value': 'john'},
                                        {'Name': 'db_pass', 'Value': 'None'}]}]

        with patch.object(self.ssm_config, 'call_boto_with_pagination',
                          side_effect=[describe_response, get_response]) as mock_call:
            result = self.ssm_config.get_credentials_by_prefix('db')

        self.assertEqual(result, {'user': 'john', 'pass': None})
        mock_call.assert_any_call('describe_parameters', ParameterFilters=[
            {'Key': 'tag:Environment', 'Values': ['dev']},
            {'Key': 'Name', 'Option': 'BeginsWith', 'Values': ['db_']}])
        mock_call.assert_called_with('get_parameters', Names=['db_user', 'db_pass'], WithDecryption=True)


    def test_get_credentials_by_prefix__nothing_found(self):
        with patch.dict(os.environ, PROD_ENV):
            ssm_config = SSMConfig()

        with patch.object(ssm_config, 'call_boto_with_pagination', return_value=[{'Parameters': []}]) as mock_call:
            with self.assertLogs(level='WARNING'):
                result = ssm_config.get_credentials_by_prefix('db_')

        self.assertEqual(result, {})
        mock_call.assert_called_once_with('describe_parameters', ParameterFilters=[
            {'Key': 'tag:Environment', 'Values': ['production']},
            {'Key': 'Name', 'Option': 'BeginsWith', 'Values': ['db_']}])


    def test_get_credentials_by_prefix__chunks_names_by_ten(self):
        describe_response = [{'Parameters': [{'Name': f'db_key_{i:02}', 'Type': 'String'} for i in range(12)]}]
        first_page = [{'Parameters': [{'Name': f'db_key_{i:02}', 'Value': str(i)} for i in range(10)]}]
        second_page = [{'Parameters': []}]

        with patch.object(self.ssm_config, 'call_boto_with_pagination',
                          side_effect=[describe_response, first_page, second_page]) as mock_call:
            result = self.ssm_config.get_credentials_by_prefix('db')

        self.assertEqual(len(result), 10)
        self.assertEqual(result['key_03'], '3')
        self.assertEqual(mock_call.call_count, 3)
        mock_call.assert_called_with('get_parameters', Names=['db_key_10', 'db_key_11'], WithDecryption=False)


class DynamoConfig_UnitTestCase(unittest.TestCase):

    def setUp(self):
        self.dynamo_config = DynamoConfig(test=True)
        self.mock_client = MagicMock()
        self.dynamo_config.dynamo_client = self.mock_client


    def test_init__test_derived_from_env(self):
        self.assertTrue(DynamoConfig().test)
        self.assertEqual(DynamoConfig().config['dynamo_client_config']['table_name'], 'autotest_config')

        with patch.dict(os.environ, PROD_ENV):
            dynamo_config = DynamoConfig()

        self.assertFalse(dynamo_config.test)
        self.assertEqual(dynamo_config.config['dynamo_client_config']['table_name'], 'config')


    def test_init__custom_config_recursively_updated(self):
        dynamo_config = DynamoConfig(test=True, config={'dynamo_client_config': {'table_name': 'custom_config'}})

        self.assertEqual(dynamo_config.config['dynamo_client_config']['table_name'], 'custom_config')
        self.assertEqual(dynamo_config.config['dynamo_client_config']['required_fields'],
                         ['env', 'config_name', 'config_value'])


    def test_get_config__json_value(self):
        self.mock_client.get_by_query.return_value = [{'config_value': '{"a": 1}'}]

        self.assertEqual(self.dynamo_config.get_config('some_config'), {'a': 1})
        self.mock_client.get_by_query.assert_called_once_with(keys={'env': 'production', 'config_name': 'some_config'})


    def test_get_config__string_value(self):
        self.mock_client.get_by_query.return_value = [{'config_value': 'some text'}]

        self.assertEqual(self.dynamo_config.get_config('some_config', env='dev'), 'some text')
        self.mock_client.get_by_query.assert_called_once_with(keys={'env': 'dev', 'config_name': 'some_config'})


    def test_get_config__not_found_returns_empty_dict(self):
        self.mock_client.get_by_query.return_value = []

        self.assertEqual(self.dynamo_config.get_config('some_config'), {})


    def test_get_config__no_ddb_access_returns_empty_dict(self):
        self.dynamo_config.dynamo_client = None
        self.dynamo_config.no_ddb_access = True

        self.assertEqual(self.dynamo_config.get_config('some_config'), {})


    def test_update_config__test_env_writes_to_dev(self):
        self.dynamo_config.update_config('some_config', 'some_value')

        self.mock_client.update.assert_called_once_with(keys={'env': 'dev', 'config_name': 'some_config'},
                                                        attributes_to_update={'config_value': 'some_value'})


    def test_update_config__production(self):
        with patch.dict(os.environ, PROD_ENV):
            dynamo_config = DynamoConfig()
            dynamo_config.dynamo_client = self.mock_client
            dynamo_config.update_config('some_config', 'some_value')

        self.mock_client.update.assert_called_once_with(keys={'env': 'production', 'config_name': 'some_config'},
                                                        attributes_to_update={'config_value': 'some_value'})


    def test_get_credentials_by_prefix(self):
        self.mock_client.get_by_query.return_value = [
            {'config_name': 'db_user', 'config_value': 'john'},
            {'config_name': 'db_settings', 'config_value': '{"port": 27019}'},
        ]

        result = self.dynamo_config.get_credentials_by_prefix('db')

        self.assertEqual(result, {'user': 'john', 'settings': {'port': 27019}})
        self.mock_client.get_by_query.assert_called_once_with(keys={'env': 'dev', 'config_name': 'db_'},
                                                              comparisons={'config_name': 'begins_with'})


    def test_get_credentials_by_prefix__autotest_prefix_forces_dev(self):
        with patch.dict(os.environ, PROD_ENV):
            dynamo_config = DynamoConfig()
        dynamo_config.dynamo_client = self.mock_client
        self.mock_client.get_by_query.return_value = []

        self.assertEqual(dynamo_config.get_credentials_by_prefix('autotest_db_'), {})
        self.mock_client.get_by_query.assert_called_once_with(keys={'env': 'dev', 'config_name': 'autotest_db_'},
                                                              comparisons={'config_name': 'begins_with'})


    def test_get_credentials_by_prefix__production_env_preserved(self):
        with patch.dict(os.environ, PROD_ENV):
            dynamo_config = DynamoConfig()
        dynamo_config.dynamo_client = self.mock_client
        self.mock_client.get_by_query.return_value = []

        dynamo_config.get_credentials_by_prefix('db')

        self.mock_client.get_by_query.assert_called_once_with(keys={'env': 'production', 'config_name': 'db_'},
                                                              comparisons={'config_name': 'begins_with'})


    def test_get_credentials_by_prefix__no_ddb_access_returns_empty_dict(self):
        self.dynamo_config.dynamo_client = None
        self.dynamo_config.no_ddb_access = True

        self.assertEqual(self.dynamo_config.get_credentials_by_prefix('db'), {})
        self.mock_client.get_by_query.assert_not_called()


    def test_get_dynamo_client__initializes_once(self):
        dynamo_config = DynamoConfig(test=True)

        with patch('sosw.components.config.DynamoDbClient') as mock_ddb_client:
            first = dynamo_config._get_dynamo_client()
            second = dynamo_config._get_dynamo_client()

        self.assertIs(first, mock_ddb_client.return_value)
        self.assertIs(second, first)
        mock_ddb_client.assert_called_once_with(dynamo_config.config['dynamo_client_config'])


    def test_get_dynamo_client__failure_marks_no_ddb_access(self):
        dynamo_config = DynamoConfig(test=True)

        with patch('sosw.components.config.DynamoDbClient', side_effect=Exception("No access")) as mock_ddb_client:
            self.assertIsNone(dynamo_config._get_dynamo_client())
            self.assertTrue(dynamo_config.no_ddb_access)

            # The client must not retry initialization after a failure.
            self.assertIsNone(dynamo_config._get_dynamo_client())
            mock_ddb_client.assert_called_once()


if __name__ == '__main__':
    unittest.main()
