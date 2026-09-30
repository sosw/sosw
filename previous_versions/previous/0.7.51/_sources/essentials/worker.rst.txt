.. _Worker:

------
Worker
------

..  automodule:: sosw.worker
    :members:


Example
-------

Please find the following elementary example of Worker Lambda.

..  code-block:: python

    import logging
    from sosw import Worker
    from sosw.app import LambdaGlobals, get_lambda_handler

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    class Processor(Worker):

        DEFAULT_CONFIG = {
            'init_clients':     ['dynamo_db'],
            'dynamo_db_config': {
                'row_mapper':      {
                    'hash_col':  'S',  # Number
                    'range_col': 'N',  # String
                },
                'required_fields': ['hash_col', 'range_col'],
                'table_name':      'autotest_dynamo_db',  # If a table is not specified, this table will be used.
            }
        }

        dynamo_db_client = None


        def __call__(self, event):

            # Example of your Worker logic
            row = event.get('row')
            self.put_to_db(row)

            # Do some basic cleaning and marking `sosw` task as completed. 
            super().__call__(event)


        def put_to_db(self, row):

            self.dynamo_db_client.put(row)


        # Setting the entry point of the lambda.
        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(Processor, global_vars)

In case you inherit from the ``Worker`` you do not have to implement anything custom
for the function to be properly orchestrated. Just do not forget to call the ``super().__call__(event)``
at the end of your execution. It will automatically collect and update the ``stats`` as well as call
the ``WorkerAssistant`` Lambda function to close the task. If you want to use the self.result after the function call,
call the ``super().__call__(event, reset_result=False)``. The default behaviour is to reset ``self.result``
after each call.

