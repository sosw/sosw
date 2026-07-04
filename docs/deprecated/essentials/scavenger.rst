.. _Scavenger:

Scavenger
---------

..  warning::

    **Deprecated since sosw 3.0.0**; removed in 4.0. Native Lambda retries with DLQs or Step
    Functions ``Retry`` / ``Catch`` replace it — see the :doc:`migration guide </migration_3_0>`.

The main roles of Scavenger are:

* Find Completed tasks and archive them
* Find Expired tasks (ones invoked, but not successfully completed by Workers) and either retry or mark them as failed.


.. automodule:: sosw.scavenger
   :members:
