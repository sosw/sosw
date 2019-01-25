import boto3
import datetime
import json
import logging
import os

from math import ceil


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "1.00"
__license__ = "MIT"
__status__ = "Production"

__all__ = ['SiblingsManager']

logger = logging.getLogger()


class SiblingsManager:
    """
    This set of helpers can be used for Lambdas that want to invoke some siblings of self. Very useful for Lambdas
    processing queues and running out of time. Some good usecase you can find in the code of `es_ingest_us`

    The Role of your Lambda must have the following extra permissions to run correctly. Please note that we hardcode
    the Arn in the policy to avoid circular dependency when parsing YAML. This dependency is absolutely valid, but
    CloudFormation doesn't know how to parse it.

    | Read more:
    | https://aws.amazon.com/premiumsupport/knowledge-center/unable-validate-circular-dependency-cloudformation/

    .. code-block:: yaml

        Policies:
        - PolicyName: "YOUR_FUNCTION_NAME"
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
          - Effect: "Allow"
            Action: "cloudwatch:GetMetricStatistics"
            Resource: "*"
          - Effect: "Allow"
            Action: "lambda:InvokeFunction"
            Resource: "arn:aws:lambda:us-west-2:737060422660:function:YOUR_FUNCTION_NAME"
    """


    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.events_client = boto3.client('events')
        self.cloudwatch_client = boto3.client('cloudwatch')


    def any_events_rules_enabled(self, lambda_context):
        """
        Checks the `Status` of CloudWatch Events Rules.
        It is very important to use this checker before launching siblings.
        Otherwise, you can create an infinite autorespawning loop and waste **A LOT** of money.

        :param lambda_context:  Context object from your lambda_handler.

        :rtype: bool
        :raises ResourceNotFoundException: If Rule with the given `name` doesn't exist.
        """

        response = self.events_client.list_rules()
        logging.debug(lambda_context.invoked_function_arn)
        logging.debug(response)

        for rule in response.get('Rules', []):
            if not rule['State'] == 'ENABLED':
                continue

            targets = self.events_client.list_targets_by_rule(Rule=rule['Name']).get('Targets', [])
            logging.debug(targets)
            if any(t['Arn'] == lambda_context.invoked_function_arn for t in targets):
                logger.info(f"Function {lambda_context.invoked_function_arn} has at least one enabled rule: {rule}")
                return True

        return False


    def spawn_sibling(self, lambda_context, payload=None, force=False):
        """
        Asynchronously invokes a copy of same function to continue working.
        Should be called if there is still work left to do (ex: messages in the queue).

        Can optionally send some payload for example remaining unprocessed rows of something.
        Should be formatted as dictionary.

        .. :warning:
           Must recieve the global lambda_context set from lambda_handler.

        .. :warning:
           Very dangerous to use `force=True`! This can create infinite loops.
           Use only if you are sure what you are doing!

        :param lambda_context:  Context object from your lambda_handler.
        :param dict payload:    The payload to be put to event.
        :param bool force:      If specified True it will ignore the checks of enabled Events Rules.
        """

        payload = payload or {}
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'test_function')

        invocation_type = 'Event' if not os.environ.get('STAGE') == 'test' else 'DryRun'

        if not self.any_events_rules_enabled(lambda_context) and not force:
            logger.error("Can't call siblings because I don't find any enabled CloudWatch Rules for me.")
            return

        logger.info(f"Calling sibling of {name} with payload: {payload}")
        self.lambda_client.invoke(FunctionName=name, InvocationType=invocation_type, Payload=payload)


    def get_approximate_concurrent_executions(self, minutes_back=5, name=None):
        """
        Get approximate concurrent executions from CloudWatch Metrics.
        The value is **very** approximate and calculated as count of invocations during `minutes_back` divided
        by average duration in same period. Return value is rounded to integer using `ceil`.

        We assume that the Role has permissions to read CloudWatch.

        :param int minutes_back: Aggregate statistics for this number of minutes.
        :param str name: Name of the function to check. *Default: currently running lambda.*
        :rtype: int
        :return: Approximate number of concurrent executions.
        """

        name = name or os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'test_function')
        period = 60 * minutes_back
        st = datetime.datetime.now() - datetime.timedelta(minutes=minutes_back)
        en = datetime.datetime.now()

        # First we get the average duration.
        # Please notice that difference between StartTime and EndTime is exactly equal to `period`.
        # This makes sure that we get only one aggregated `Datapoint` in the result.
        response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName="Duration",
                StartTime=st, EndTime=en, Period=period, Statistics=['Average'],
                Dimensions=[{"Name": "FunctionName", "Value": name}])

        # If we had invocations during `period` - we have just one `Datapoint` in response.
        if response['Datapoints']:
            average_duration = response['Datapoints'][0]['Average']

            # Now getting the number of invocations with same filters and period.
            response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/Lambda', MetricName="Invocations",
                    StartTime=st, EndTime=en, Period=period, Statistics=['Sum'],
                    Dimensions=[{"Name": "FunctionName", "Value": name}])

            # We assume that if we have Duration, we definitely have invocations.
            # You may change this if you catch some VALID example where this is not True.
            number_of_invocations = response['Datapoints'][0]['Sum']

            return ceil(number_of_invocations * (average_duration / (1000 * period)))
        else:
            logger.info(f"Failed to get data from CW (or no data) for {name}.")
            return 0
