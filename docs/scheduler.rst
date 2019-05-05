.. _Scheduler:

Scheduler
---------

Scheduler is the public interface of SOSW for any applications who want to invoke some orchestrated Lambdas.
It's main role is to transform some business job to the actual payload of Lambda invocations. It respects the
configurable rules for chunking specific for different workers.

.. figure:: images/scheduler.png
   :alt: sosw Scheduler Workflow
   :align: center

   Scheduler Workflow


.. automodule:: sosw.scheduler
   :members:
