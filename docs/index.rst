sosw
====

Serverless Orchestrator of Serverless Workers

`sosw` is the set of tools for orchestrating **asynchronous** invocations of AWS Lambda Workers.

`sosw` requires you to implement/deploy several Lambda functions (Essentials) using the following core classes:

.. toctree::
   :titlesonly:

   orchestrator
   scheduler
   scavenger
   worker_assistant

The Worker functions themselves are expected to call the WorkerAssistant when completed the task
from each invocation. If you inherit the ``sosw.Worker`` class in your function the ``__call__`` method
does that automatically. And there are several other common features that Worker class provides
(statistic aggregator, components initialisation, configuration automatic assembling and more...)

Another deployment requirement is creating several `DynamoDB` tables:

- ``sosw_tasks``
- ``sosw_retry_tasks``
- ``sosw_closed_tasks``

You can find the Cloudformation template for the databases in the
`in the example<https://raw.githubusercontent.com/bimpression/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml>`_


.. toctree::
   :titlesonly:
   :caption: Contents:

   convention
   components
   managers

   processor
   orchestrator
   scheduler
   scavenger
   worker


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
