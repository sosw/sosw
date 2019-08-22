Siblings
--------

.. warning:: This components has performance issues unless running with ``force=True``.
   Can cause CloudWatch requests throttling. Requires some refactoring.


SiblingsManager provides Lambda executions an option of a "healthy" shutdown.
They may pass the remaining payload to another execution automatically. See example:

.. code-block:: python

   import logging
   import time
   from sosw import Processor as SoswProcessor
   from sosw.app import LambdaGlobals, get_lambda_handler
   from sosw.components.siblings import SiblingsManager

   logger = logging.getLogger()
   logger.setLevel(logging.INFO)

   class Processor(SoswProcessor):
   
       DEFAULT_CONFIG = {
           'init_clients':    ['Siblings'],    # Automatically initialize Siblings Manager
           'shutdown_period': 10,  # Some time to shutdown in a healthy manner.
       }
   
       siblings_client: SiblingsManager = None
   
   
       def __call__(self, event):
   
           cursor = event.get('cursor', 0)
   
           while self.sufficient_execution_time_left:
               self.process_data(cursor)
               cursor += 1
               if cursor == 20:
                   return f"Reached the end of data"
   
           else:
               # Spawning another sibling to continue the processing
               payload = {'cursor': cursor}
   
               self.siblings_client.spawn_sibling(global_vars.lambda_context, payload=payload, force=True)
               self.stats['siblings_spawned'] += 1
   
   
       def process_data(self, cursor):
           """ Your custom logic respecting current cursor. """
           logger.info(f"Processing data at cursor: {cursor}")
           time.sleep(1)
   
   
       @property
       def sufficient_execution_time_left(self) -> bool:
           """ Return whether there is a sufficient execution time for processing ('shutdown period' is in seconds). """
           return global_vars.lambda_context.get_remaining_time_in_millis() > self.config['shutdown_period'] * 1000
   
   
   global_vars = LambdaGlobals()
   lambda_handler = get_lambda_handler(Processor, global_vars)


Here is an example use-case when you can store the remaining payload for example in S3 and call the sibling with a pointer to it.

.. automodule:: sosw.components.siblings
   :members:
