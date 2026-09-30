import logging
import os
import unittest
from unittest.mock import patch, MagicMock


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.config import SecretsManager


class secretsmanager_client_UnitTestCase(unittest.TestCase):

    def setUp(self):
        self.secretsmanager_obj = SecretsManager()
        self.mock_client = MagicMock()
        self.secretsmanager_obj.secretsmanager_client = self.mock_client


    def test_init__test_derived_from_env(self):
        self.assertTrue(SecretsManager().test)
        self.assertTrue(SecretsManager(test=True).test)

        with patch.dict(os.environ, {'STAGE': 'production', 'autotest': 'False'}):
            self.assertFalse(SecretsManager().test)


    def test_get_secretsmanager_client__initializes_once(self):
        secrets_manager = SecretsManager()

        with patch('sosw.components.config.boto3.client') as mock_boto_client:
            first = secrets_manager._get_secretsmanager_client()
            second = secrets_manager._get_secretsmanager_client()

        self.assertIs(first, mock_boto_client.return_value)
        self.assertIs(second, first)
        mock_boto_client.assert_called_once_with('secretsmanager')


    def test_call_boto_secrets_with_pagination__native_paginator(self):
        self.mock_client.can_paginate.return_value = True
        paginator = self.mock_client.get_paginator.return_value
        pages = [{'SecretList': [{'Name': 'some_secret'}]}]
        paginator.paginate.return_value = pages

        result = self.secretsmanager_obj.call_boto_secrets_with_pagination('list_secrets', Filters=[])

        self.assertIs(result, pages)
        self.mock_client.can_paginate.assert_called_once_with('list_secrets')
        self.mock_client.get_paginator.assert_called_once_with('list_secrets')
        paginator.paginate.assert_called_once_with(Filters=[])


    def test_call_boto_secrets_with_pagination__manual_pagination(self):
        self.mock_client.can_paginate.return_value = False
        self.mock_client.describe_secret.side_effect = [{'Name': 'a'}]

        result = self.secretsmanager_obj.call_boto_secrets_with_pagination('describe_secret', SecretId='some_id')

        self.assertEqual(result, [{'Name': 'a'}])
        self.mock_client.describe_secret.assert_called_once_with(SecretId='some_id')


    def test_call_boto_secrets_with_pagination__manual_pagination_with_token(self):
        """
        A response carrying a `NextToken` must trigger a follow-up call with the token of the latest page.
        All pages accumulate in the result and the loop terminates on the first page without a token.
        """

        self.mock_client.can_paginate.return_value = False
        self.mock_client.describe_secret.side_effect = [{'Name': 'a', 'NextToken': 'token_1'},
                                                        {'Name': 'b', 'NextToken': 'token_2'},
                                                        {'Name': 'c'}]

        result = self.secretsmanager_obj.call_boto_secrets_with_pagination('describe_secret', SecretId='some_id')

        self.assertEqual(result, [{'Name': 'a', 'NextToken': 'token_1'},
                                  {'Name': 'b', 'NextToken': 'token_2'},
                                  {'Name': 'c'}])
        self.assertEqual(self.mock_client.describe_secret.call_count, 3)
        self.mock_client.describe_secret.assert_any_call(SecretId='some_id', NextToken='token_1')
        self.mock_client.describe_secret.assert_called_with(SecretId='some_id', NextToken='token_2')


    def test_key_error(self):
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'a': 'b'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'tag': 'v', 'value': 'test'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'type': '', 'value': 'test'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'type': 'name'})
        self.assertRaises(KeyError, self.secretsmanager_obj.get_secrets_credentials, **{'type': 'name', 'value': ''})


    def test_get_secrets_credentials__by_name(self):
        self.mock_client.get_secret_value.return_value = {'SecretString': 'sh-h-h'}
        pages = [{'SecretList': [{'ARN': 'arn:secret:1', 'Name': 'db_password'}]}]

        with patch.object(self.secretsmanager_obj, 'call_boto_secrets_with_pagination',
                          return_value=pages) as mock_call:
            result = self.secretsmanager_obj.get_secrets_credentials(type='name', value='db_password')

        self.assertEqual(result, {'db_password': 'sh-h-h'})
        mock_call.assert_called_once_with('list_secrets', Filters=[{'Key': 'name', 'Values': ['db_password']}])
        self.mock_client.get_secret_value.assert_called_once_with(SecretId='arn:secret:1')


    def test_get_secrets_credentials__by_tag(self):
        self.mock_client.get_secret_value.side_effect = [{'SecretString': 'first'}, {'SecretString': 'second'}]
        pages = [{'SecretList': [{'ARN': 'arn:secret:1', 'Name': 'db_user'}]},
                 {'SecretList': [{'ARN': 'arn:secret:2', 'Name': 'db_password'}]}]

        with patch.object(self.secretsmanager_obj, 'call_boto_secrets_with_pagination',
                          return_value=pages) as mock_call:
            result = self.secretsmanager_obj.get_secrets_credentials(type='tag', value='project_a')

        self.assertEqual(result, {'db_user': 'first', 'db_password': 'second'})
        mock_call.assert_called_once_with('list_secrets', Filters=[{'Key': 'tag-value', 'Values': ['project_a']}])


    def test_get_secrets_credentials__nothing_found(self):
        with patch.object(self.secretsmanager_obj, 'call_boto_secrets_with_pagination',
                          return_value=[{'SecretList': []}]):
            with self.assertLogs(level='WARNING'):
                result = self.secretsmanager_obj.get_secrets_credentials(type='name', value='missing')

        self.assertEqual(result, {})
        self.mock_client.get_secret_value.assert_not_called()


if __name__ == '__main__':
    unittest.main()
