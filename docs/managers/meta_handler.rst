.. _meta_handler:

MetaHandler
-----------

Meta Handler is an optional collector of all the events that were happening to the task while being orchestrated by
``sosw``. In case you create a DynamoDB table ``sosw_tasks_meta`` in your environment, ``sosw`` will start collecting
the meta data  automatically. The required schema for the table is described in
`sosw/examples/yaml/initial/sosw-dev-shared-dynamodb.yaml
<https://raw.githubusercontent.com/sosw/sosw/master/examples/yaml/initial/sosw-dev-shared-dynamodb.yaml>`_ .

The setup script will create the table by default.

MetaHandler can also be automatically initialised by classes, inheriting from ``Worker``. They will then write
several events to the meta table as well. To enable that you have to provide these Lambdas with some details
in the ``meta_handler_config`` of their custom config. And make sure they have write permissions for this table.

Config example:

..  code-block:: python

    {
        'meta_handler_config': {
            'write_meta_to_ddb': True,
            'dynamo_db_config': {
                'table_name':      'sosw_tasks_meta'
            }
        }
    }


.. automodule:: sosw.managers.meta_handler
   :members:
