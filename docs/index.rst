.. title:: Home

====================================================
sosw — bootstrap AWS Lambda functions
====================================================

..  image:: https://github.com/sosw/sosw/actions/workflows/run-unittests.yml/badge.svg
    :alt: Tests
    :target: https://github.com/sosw/sosw/actions/workflows/run-unittests.yml
..  image:: https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml/badge.svg
    :alt: Docs
    :target: https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml
..  image:: _static/images/coverage.svg
    :alt: Test Coverage
    :target: https://docs.sosw.app/?badge=latest
..  image:: https://img.shields.io/pypi/dm/sosw?color=blue&label=pypi%20installs
    :alt: PyPI - Installs / month
    :target: https://pypi.org/project/sosw/
..  image:: https://img.shields.io/pypi/l/sosw?color=blue
    :alt: PyPI - License
    :target: https://github.com/sosw/sosw/blob/master/LICENSE

**sosw** is a Python framework for bootstrapping AWS Lambda functions.

It gives every Lambda in your account the same production-grade skeleton in a dozen lines of code:

* :ref:`Processor` — a base class with layered configuration (code defaults + DynamoDB / SSM overrides),
  automatic initialization of AWS clients, statistics counters and a uniform entry point;
* **warm start** — the generated ``lambda_handler`` caches the initialized Processor for the lifetime
  of the Lambda container, so warm invocations skip all the initialization work
  (see :ref:`Warm start <Warm Start>`);
* :doc:`LambdaApi <lambda_api>` — a declarative router base class for Lambdas behind API Gateway
  (routes, Cognito claims, CORS, uniform JSON error envelopes);
* :doc:`durable functions support <durable>` — an optional wrapper integrating the AWS Lambda
  Durable Execution SDK (``pip install sosw[durable]``);
* **components and helpers** — battle-tested middleware for DynamoDB, SNS, configuration sources,
  SigV4-signed requests, sibling invocations and a large :doc:`helpers library <components/helpers>`.

The only mandatory runtime dependency is ``boto3``. Python 3.10 – 3.14 are supported.

..  warning::

    ``sosw`` began life as the *Serverless Orchestrator of Serverless Workers*. Since version
    **3.0.0** the self-hosted orchestration layer (``Orchestrator``, ``Scheduler``, ``Scavenger``,
    ``Worker`` and their managers) is **deprecated**: it stays fully functional throughout 3.x and
    will be removed in 4.0. New designs should use AWS Step Functions, EventBridge Scheduler or
    durable functions instead. See the :doc:`migration guide <migration_3_0>` and the preserved
    :doc:`orchestration documentation <deprecated/index>`.

Install it and build your first function in minutes:

..  code-block:: bash

    pip install sosw

..  note::
    Please pronounce ``sosw`` correctly: */ˈsɔːsəʊ/*

..  toctree::
    :titlesonly:
    :caption: Contents:
    :maxdepth: 2

    quickstart
    concepts/index
    lambda_api
    durable
    components/index
    tools/index
    tutorials/index
    migration_3_0

    contribution/index
    deprecated/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
