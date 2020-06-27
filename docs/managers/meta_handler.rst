MetaHandler
-----------

Meta Handler is an optional collector of all the events that were happening to the task while being orchestrated by
``sosw``. In case you create a DynamoDB table ``sosw_tasks_meta`` in your environment, ``sosw`` will start collecting
the meta data  automatically. The required schema for the table is described in
`sosw/examples/yaml/initial/sosw-dev-shared-dynamodb.yaml
<https://raw.githubusercontent.com/sosw/sosw/master/examples/yaml/initial/sosw-dev-shared-dynamodb.yaml>`_ .

The setup script will create the table by default.

.. automodule:: sosw.managers.meta_handler
   :members:
