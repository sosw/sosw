.. _Worker:

------
Worker
------

..  automodule:: sosw.worker
    :members:


Example
-------

Please find the following elementary example of Worker Lambda.

.. code-block:: python

    import logging
    from sosw import Worker

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
        lambda_handler = get_lambda_handler(Processor)
