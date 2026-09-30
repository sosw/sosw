import unittest
import os

from unittest.mock import MagicMock
from unittest import mock


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class siblings_TestCase(unittest.TestCase):

    CUSTOM_CONFIG = {
        "test": True
    }

    @mock.patch("boto3.client")
    def test_get_approximate_concurrent_executions(self, mock_boto_client):
        mock_get_metric_statistics_responses = [
            {
                'Datapoints':               [
                    {
                        'Average': 1.0,  # Extremely short - 1ms.
                        'Sum':     5  # Fake invocations
                    }
                ],
                'function_expected_result': 1
            },
            {
                'Datapoints':               [
                    {
                        'Average': 55000.0,  # A little shorter than 1 minute
                        'Sum':     5
                    }
                ],
                'function_expected_result': 1
            },
            {
                'Datapoints':               [
                    {
                        'Average': 65000.0,  # A little longer than 1 minute, should be ceiled to 2 minutes
                        'Sum':     5
                    }
                ],
                'function_expected_result': 2
            },
            {
                'Datapoints':               [
                    {
                        'Average': 120000.0,  # Exactly 2 minutes.
                        'Sum':     5
                    }
                ],
                'function_expected_result': 2
            },
            {
                'Datapoints':               [
                    {
                        'Average': 300000.0,  # Exactly 5 minutes, but a single invocation
                        'Sum':     1
                    }
                ],
                'function_expected_result': 1
            },
            {
                'Datapoints':               [
                    {
                        'Average': 300000.0,  # Exactly 5 minutes, invoked every minute
                        'Sum':     5
                    }
                ],
                'function_expected_result': 5
            },
        ]

        client = MagicMock()
        for experiment in mock_get_metric_statistics_responses:
            client.get_metric_statistics = MagicMock(return_value=experiment, side_effect=None)
            mock_boto_client.return_value = client

            # Reimport the component
            from sosw.components.siblings import SiblingsManager

            self.assertEqual(SiblingsManager(custom_config=self.CUSTOM_CONFIG).get_approximate_concurrent_executions(),
                             experiment['function_expected_result'])


    @mock.patch("boto3.client")
    def test_any_events_rules_enabled(self, mock_boto_client_v2):
        """
        Two functions:
        * my-test-func1 has only 1 rule DISABLED
        * my-test-func2 has 2 rules, one ENABLED, one DISABLED
        """


        def my_side_effect(**kwargs):

            if kwargs['Rule'] == 'test-rule-1':
                return {'Targets': [{'Arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func1'}]}
            elif kwargs['Rule'] == 'test-rule-2':
                return {'Targets': [{'Arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func2'}]}
            elif kwargs['Rule'] == 'test-rule-3':
                return {'Targets': [{'Arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func2'}]}


        mock_list_rules = {
            'Rules': [{
                'Arn':                'arn:aws:events:us-west-2:123:rule/test-rule-1',
                'Description':        'Test rule',
                'Name':               'test-rule-1',
                'ScheduleExpression': 'rate(5 minutes)',
                'State':              'DISABLED'
            }, {
                'Arn':                'arn:aws:events:us-west-2:123:rule/test-rule-2',
                'Description':        'Test rule',
                'Name':               'test-rule-2',
                'ScheduleExpression': 'rate(5 minutes)',
                'State':              'DISABLED'
            },
                {
                    'Arn':                'arn:aws:events:us-west-2:123:rule/test-rule-3',
                    'Description':        'Test rule',
                    'Name':               'test-rule-3',
                    'ScheduleExpression': 'rate(5 minutes)',
                    'State':              'ENABLED'
                }]
        }

        client2 = MagicMock()

        client2.list_rules = MagicMock(return_value=mock_list_rules)
        client2.list_targets_by_rule = MagicMock(side_effect=my_side_effect)
        mock_boto_client_v2.return_value = client2

        # Reimport the component
        from sosw.components.siblings import SiblingsManager

        self.assertFalse(SiblingsManager(custom_config=self.CUSTOM_CONFIG).any_events_rules_enabled(type('lambda_context', (object,), {
            'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func1'
        })))

        self.assertTrue(SiblingsManager(custom_config=self.CUSTOM_CONFIG).any_events_rules_enabled(type('lambda_context', (object,), {
            'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func2'
        })))

        # Testing auto_spawning defaults from config. Copies of the config: mutating the shared
        # class-level CUSTOM_CONFIG would leak into other tests and suite reruns in the same process.
        self.assertFalse(SiblingsManager(custom_config={**self.CUSTOM_CONFIG, 'auto_spawning': False})
                         .any_events_rules_enabled(type('lambda_context', (object,), {
                             'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func1'
                         })))

        self.assertTrue(SiblingsManager(custom_config={**self.CUSTOM_CONFIG, 'auto_spawning': True})
                        .any_events_rules_enabled(type('lambda_context', (object,), {
                            'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func1'
                        })))


    @mock.patch("boto3.client")
    def test_spawn_sibling(self, mock_boto_client):
        """
        With enabled Events Rules the sibling is invoked with the JSON-dumped payload.
        The STAGE=test environment enforces the DryRun invocation type.
        """

        client = MagicMock()
        mock_boto_client.return_value = client

        from sosw.components.siblings import SiblingsManager

        manager = SiblingsManager(custom_config=self.CUSTOM_CONFIG)
        manager.any_events_rules_enabled = MagicMock(return_value=True)

        manager.spawn_sibling(MagicMock(), payload={'rows': [1, 2]})

        manager.lambda_client.invoke.assert_called_once_with(
                FunctionName='test_function', InvocationType='DryRun', Payload='{"rows": [1, 2]}')


    @mock.patch("boto3.client")
    def test_spawn_sibling__no_rules_enabled(self, mock_boto_client):
        """
        Without enabled Events Rules (and without force) the sibling must NOT be invoked.
        """

        client = MagicMock()
        mock_boto_client.return_value = client

        from sosw.components.siblings import SiblingsManager

        manager = SiblingsManager(custom_config=self.CUSTOM_CONFIG)
        manager.any_events_rules_enabled = MagicMock(return_value=False)

        manager.spawn_sibling(MagicMock())

        manager.lambda_client.invoke.assert_not_called()


    @mock.patch("boto3.client")
    def test_spawn_sibling__force_skips_rules_check(self, mock_boto_client):
        """
        `force=True` must skip the check of Events Rules. A string payload is passed through as is.
        """

        client = MagicMock()
        mock_boto_client.return_value = client

        from sosw.components.siblings import SiblingsManager

        manager = SiblingsManager(custom_config=self.CUSTOM_CONFIG)
        manager.any_events_rules_enabled = MagicMock(return_value=False)

        manager.spawn_sibling(MagicMock(), payload='{"file_name": "some.txt"}', force=True)

        manager.any_events_rules_enabled.assert_not_called()
        manager.lambda_client.invoke.assert_called_once_with(
                FunctionName='test_function', InvocationType='DryRun', Payload='{"file_name": "some.txt"}')


    @mock.patch("boto3.client")
    def test_get_approximate_concurrent_executions__no_datapoints(self, mock_boto_client):
        """
        Without CloudWatch Datapoints (no recent invocations) the estimate is zero.
        """

        client = MagicMock()
        client.get_metric_statistics = MagicMock(return_value={'Datapoints': []})
        mock_boto_client.return_value = client

        from sosw.components.siblings import SiblingsManager

        manager = SiblingsManager(custom_config=self.CUSTOM_CONFIG)

        self.assertEqual(manager.get_approximate_concurrent_executions(), 0)
        client.get_metric_statistics.assert_called_once()


if __name__ == '__main__':
    unittest.main()
