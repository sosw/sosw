Worker
------

Worker should be used as parent class for your Lambda processors in your workers.
It has all the common methods of `sosw.app.Processor` and tries to close task in case it received some
`task_id` in the payload (event).

.. automodule:: sosw.worker
   :members:
