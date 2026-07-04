==========
Essentials
==========

..  warning::

    **Deprecated since sosw 3.0.0.** The Essentials stay functional through 3.x and will be
    removed in 4.0. See the :doc:`migration guide </migration_3_0>`.

The Essentials are the AWS Lambda functions implementing the ``sosw`` orchestration layer itself,
built on the same :ref:`Processor <Processor>` base class as your functions. Two child classes
serve as bases: ``Essential`` implements the methods and properties for the ``sosw`` essential
Lambdas, while ``Worker`` carries the mechanisms for custom functions being orchestrated by
``sosw``.

..  figure:: /_static/images/core-classes.png
    :alt: sosw core classes inheritance
    :align: center

..  toctree::
    :titlesonly:
    :caption: Essentials:

    worker
    essential

    orchestrator
    scheduler
    scavenger
    worker_assistant
