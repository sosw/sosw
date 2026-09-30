.. _Concepts:

========
Concepts
========

``sosw`` is built around one central idea: every AWS Lambda function deserves the same skeleton —
layered configuration, initialized clients, statistics, and a handler that survives warm starts.

* :doc:`processor` — the base class of your Lambda: configuration assembly, client registration,
  the ``stats`` / ``result`` contract.
* :doc:`warm_start` — how ``get_lambda_handler`` and ``LambdaGlobals`` cache the Processor for the
  lifetime of the Lambda container, and what that means for your code.
* :doc:`configuration` — where the configuration comes from: code defaults, the DynamoDB ``config``
  table, SSM Parameter Store, Secrets Manager, and ``custom_config``.

..  toctree::
    :titlesonly:
    :caption: Concepts:

    processor
    warm_start
    configuration
