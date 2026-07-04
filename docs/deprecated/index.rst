.. _Deprecated Orchestration:

==========================
Deprecated: orchestration
==========================

..  warning::

    Everything documented in this section is **deprecated since sosw 3.0.0**. The orchestration
    layer stays fully functional through every 3.x release (instantiating its classes emits a
    ``DeprecationWarning``) and will be **removed in sosw 4.0**. Do not build new systems on it —
    use AWS Step Functions, EventBridge Scheduler or :doc:`durable functions </durable>` instead.
    Migration guidance for every entity: :doc:`/migration_3_0`.

``sosw`` originally stood for *Serverless Orchestrator of Serverless Workers*: a self-hosted
orchestration layer in which special "Essential" Lambdas (:ref:`Orchestrator`,
:ref:`Scheduler`, :ref:`Scavenger`, :ref:`Worker Assistant`) scheduled, invoked, throttled,
retried and archived tasks for your :ref:`Worker` Lambdas through a set of DynamoDB queue tables.

This section preserves the complete documentation of that layer for the teams still operating it.

..  toctree::
    :titlesonly:
    :caption: Orchestration (deprecated):

    orchestration
    installation
    essentials/index
    managers/index
    greenfield
    tutorials/sosw_tutorial_pull_tweeter_hashtags
    tutorials/cleanup
