.. _Tutorials:

=========
Tutorials
=========

End-to-end walkthroughs building real functions on ``sosw`` 3.0:

* :doc:`first_lambda` — scaffold, implement, unit-test, deploy and invoke a Processor-based
  Lambda with AWS SAM.
* :doc:`http_api` — put a declarative :doc:`LambdaApi <../lambda_api>` behind an API Gateway
  HTTP API with a Cognito JWT authorizer, and verify the whole error contract with ``curl``.

..  toctree::
    :titlesonly:
    :caption: Tutorials:

    first_lambda
    http_api

..  note::

    The historical tutorials of the deprecated orchestration layer (pulling Twitter hashtags with
    Workers, Essentials cleanup) are preserved under
    :doc:`Deprecated: orchestration <../deprecated/index>`.
