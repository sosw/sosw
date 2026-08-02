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
``'completed'`` and ``'failed'`` events to the DynamoDB tasks meta data table. In order to enable this feature,
you have to provide ``'meta_handler_config'`` in your custom_config. You will also need to grant write
permissions for this table to your Lambda.

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
