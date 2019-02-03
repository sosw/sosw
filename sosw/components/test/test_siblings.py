import unittest
import os

from unittest.mock import MagicMock
from unittest import mock


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class siblings_TestCase(unittest.TestCase):

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
            from ..siblings import SiblingsManager

            self.assertEqual(SiblingsManager().get_approximate_concurrent_executions(),
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
        from ..siblings import SiblingsManager

        self.assertFalse(SiblingsManager().any_events_rules_enabled(type('lambda_context', (object,), {
            'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func1'
        })))

        self.assertTrue(SiblingsManager().any_events_rules_enabled(type('lambda_context', (object,), {
            'invoked_function_arn': 'arn:aws:lambda:us-west-2:123:function:my-test-func2'
        })))


    @mock.patch("boto3.client")
    def test_spawn_sibling(self, mock_boto_client):
        client = MagicMock()

        client.list_rules = MagicMock(return_value={})
        client.list_targets_by_rule = MagicMock(return_value={})

        mock_boto_client.return_value = client
        # Test here that sibling is spawned only if rule enabled.


if __name__ == '__main__':
    unittest.main()
