.. _First sosw Lambda:

======================
Your first sosw Lambda
======================

In this tutorial you will build, unit-test, deploy and invoke a complete AWS Lambda function on
``sosw`` — from an empty directory to a running function — using
`AWS SAM <https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html>`_.

The function itself is deliberately simple: it receives a greeting request and answers with the
identity of the AWS account it runs in — enough to exercise every framework mechanism you will
use in real functions: configuration, automatic clients, statistics, warm start and unit tests.


Prerequisites
-------------

* An AWS account and credentials configured for the AWS CLI.
* `SAM CLI <https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html>`_
  installed.
* Python 3.12+ (the examples use 3.14, matching the Lambda runtime).


1. Project layout
-----------------

..  code-block:: bash

    mkdir -p hello-sosw/src hello-sosw/tests
    cd hello-sosw

..  code-block:: text

    hello-sosw/
    ├── src/
    │   ├── app.py              # the Processor and the lambda_handler
    │   └── requirements.txt    # sosw
    ├── tests/
    │   └── test_app.py         # unit tests, no network
    └── template.yaml           # SAM template

``src/requirements.txt`` is a single line:

..  code-block:: text

    sosw


2. The Processor
----------------

Create ``src/app.py``:

..  code-block:: python

    """hello-sosw: my first Lambda bootstrapped with sosw."""

    import logging

    from sosw.app import LambdaGlobals, get_lambda_handler, Processor as SoswProcessor
    from sosw.components.benchmark import benchmark

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)


    class Processor(SoswProcessor):

        DEFAULT_CONFIG = {
            'disable_ddb_config': True,       # No external config lookup (yet) - see step 6.
            'init_clients':       ['sts'],    # Automatically initialize `self.sts_client`.
            'greeting':           'Hello',
        }

        sts_client = None


        def __call__(self, event, **kwargs):
            super().__call__(event)

            name = event.get('name', 'world')
            account = self.get_account()
            self.result['greetings_sent'] += 1

            return {
                'message': f"{self._c('greeting')}, {name}! Running in account {account}.",
                'stats':   self.get_stats(),
            }


        @benchmark
        def get_account(self):
            return self.sts_client.get_caller_identity()['Account']


    global_vars = LambdaGlobals()
    lambda_handler = get_lambda_handler(Processor, global_vars)

Things to notice:

* ``init_clients: ['sts']`` — no matching sosw component exists, so the Processor falls back to a
  plain ``boto3.client('sts')`` and assigns it to ``self.sts_client``. One line instead of the
  usual client plumbing.
* ``self._c('greeting')`` reads from the layered config — you will override it without a
  redeployment in step 6.
* ``disable_ddb_config: True`` keeps the cold start free of the DynamoDB config lookup until we
  actually create the table.
* ``lambda_handler`` is generated once at import time; the Processor instance survives warm
  invocations (see :ref:`Warm start <Warm Start>`).


3. Unit tests before deployment
-------------------------------

Create ``tests/test_app.py``. ``sosw`` functions are designed to be tested without any AWS access:

..  code-block:: python

    import os
    import sys
    import unittest

    from unittest.mock import MagicMock, patch

    os.environ['STAGE'] = 'test'
    os.environ['autotest'] = 'True'

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

    from app import Processor


    class ProcessorTestCase(unittest.TestCase):

        @patch('boto3.client')
        def setUp(self, mock_boto_client):
            self.processor = Processor()

            self.processor.sts_client = MagicMock()
            self.processor.sts_client.get_caller_identity.return_value = {'Account': '000000000000'}


        def test_call__greets(self):
            result = self.processor({'name': 'sosw'})

            self.assertEqual(result['message'], "Hello, sosw! Running in account 000000000000.")


        def test_call__counts_stats(self):
            self.processor({})

            self.assertEqual(self.processor.stats['processor_calls'], 1)
            self.assertEqual(self.processor.stats['calls_get_account'], 1)   # From @benchmark.


    if __name__ == '__main__':
        unittest.main()

..  code-block:: bash

    pip install sosw
    python -m unittest discover tests -v

Since this Processor sets ``disable_ddb_config``, there is no config lookup to mock. For
Processors that *do* use the config table, add
``with patch.object(Processor, 'get_config', return_value={}):`` around the construction —
see the :ref:`Quickstart <Quickstart>`.


4. The SAM template
-------------------

Create ``template.yaml``:

..  code-block:: yaml

    AWSTemplateFormatVersion: '2010-09-09'
    Transform: AWS::Serverless-2016-10-31
    Description: hello-sosw - my first Lambda bootstrapped with sosw.

    Resources:

      HelloSoswFunction:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: hello-sosw
          CodeUri: src/
          Handler: app.lambda_handler
          Runtime: python3.14
          Timeout: 30
          MemorySize: 256
          Environment:
            Variables:
              STAGE: production

    Outputs:
      FunctionArn:
        Value: !GetAtt HelloSoswFunction.Arn

..  note::

    Larger projects usually mount ``sosw`` from a shared Lambda Layer instead of packaging it
    into every function — see :ref:`SOSW Layer`.


5. Deploy and invoke
--------------------

..  code-block:: bash

    sam build
    sam deploy --guided          # first time; accept the defaults, stack name: hello-sosw

    aws lambda invoke --function-name hello-sosw \
        --cli-binary-format raw-in-base64-out \
        --payload '{"name": "sosw"}' /dev/stdout

You should see your greeting with the real account id, plus the ``stats`` counters — including
the ``@benchmark`` timing of ``get_account``. Invoke it twice and compare: the second (warm)
invocation carries ``total_*`` accumulators from the first one and skips initialization entirely.

Check the logs:

..  code-block:: bash

    sam logs --name HelloSoswFunction --stack-name hello-sosw

The handler logs the assembled config on the cold start and the stats of every invocation.


6. Reconfigure without redeploying (optional)
---------------------------------------------

Create the DynamoDB ``config`` table (schema in :ref:`Configuration <Configuration>`), grant the
function role ``dynamodb:Query`` on it, and remove the ``disable_ddb_config`` line from
``DEFAULT_CONFIG``. After a redeploy, the Processor will look for the record
``config_name = 'hello-sosw_config'`` on every cold start. Try it:

..  code-block:: bash

    aws dynamodb put-item --table-name config --item '{
        "env":          {"S": "production"},
        "config_name":  {"S": "hello-sosw_config"},
        "config_value": {"S": "{\"greeting\": \"Bonjour\"}"}
    }'

New containers now greet in French — no deployment involved. This layering is the single most
operationally useful habit of ``sosw`` functions: code ships defaults, the table holds the knobs.


7. Cleanup
----------

..  code-block:: bash

    sam delete --stack-name hello-sosw


Next steps
----------

* Serve HTTP with the same pattern: :doc:`http_api`.
* Long-running workflows with checkpoints: :doc:`durable functions <../durable>`.
* The full concepts behind what you just used: :doc:`../concepts/index`.
