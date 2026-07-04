.. _Configuration:

=============
Configuration
=============

Every ``sosw`` Processor assembles its configuration from up to three layers
(see :ref:`Processor <Processor>` for the pipeline):

#.  ``DEFAULT_CONFIG`` — code-level defaults in your Processor class;
#.  an optional **per-function config** looked up in DynamoDB or SSM Parameter Store;
#.  ``custom_config`` — passed to the constructor, wins over everything.

Each layer *recursively* updates the previous one (nested dictionaries are merged, not replaced),
so an external config record only needs to carry the keys it overrides.


The DynamoDB ``config`` table
-----------------------------

The default external source is the DynamoDB table ``config`` in the region of your Lambda,
with this schema:

..  code-block:: python

    'env':          'S',    # hash key, usually 'production'
    'config_name':  'S',    # range key
    'config_value': 'S',    # JSON string

Every Processor looks for the record ``config_name = '{AWS_LAMBDA_FUNCTION_NAME}_config'``
(e.g. ``'my-first-sosw-function_config'``) and recursively merges the JSON from its
``config_value`` over ``DEFAULT_CONFIG``. This lets you re-configure a deployed Lambda —
feature flags, table names, chunk sizes, per-environment settings — without redeploying code.

A minimal CloudFormation resource for the table:

..  code-block:: yaml

    ConfigDynamoTable:
      Type: AWS::DynamoDB::Table
      Properties:
        TableName: config
        BillingMode: PAY_PER_REQUEST
        AttributeDefinitions:
          - {AttributeName: env,         AttributeType: S}
          - {AttributeName: config_name, AttributeType: S}
        KeySchema:
          - {AttributeName: env,         KeyType: HASH}
          - {AttributeName: config_name, KeyType: RANGE}

The Lambda execution role needs ``dynamodb:Query`` on this table.

..  note::

    In test mode (``STAGE=test``) the lookup automatically switches to the ``autotest_config``
    table. Unit tests normally bypass the lookup entirely by patching
    ``Processor.get_config`` — see the :ref:`Quickstart <Quickstart>` test example.


Skipping the lookup
-------------------

Functions that do not use runtime overrides (or have no access to the table) should skip the
lookup — it saves a DynamoDB query (or a failed attempt) on every cold start:

..  code-block:: python

    class Processor(SoswProcessor):
        DISABLE_DDB_CONFIG = True                        # class attribute, or:
        DEFAULT_CONFIG = {'disable_ddb_config': True}    # config flag (custom_config works too)

``DEFAULT_CONFIG`` and ``custom_config`` are still applied. New in 3.0.0.


SSM Parameter Store and Secrets Manager
---------------------------------------

The lookup is implemented by the strategy adapter ``sosw.components.config.ConfigSource`` with
two interchangeable backends — ``DynamoConfig`` (default) and ``SSMConfig`` — plus a
``SecretsManager`` client for credentials. The module-level convenience functions are
automatically wired to the default source:

..  code-block:: python

    from sosw.components.config import (get_config, update_config,
                                        get_credentials_by_prefix, get_secrets_credentials)

    last_row = get_config('my_processing_cursor')
    update_config('my_processing_cursor', last_row + 10)

    db_settings = get_credentials_by_prefix('db_')                     # SSM SecureStrings
    secret = get_secrets_credentials(type='name', value='db_password') # AWS Secrets Manager

Your Lambda role needs the relevant permissions (``dynamodb:*Item``/``Query`` on ``config``,
``ssm:GetParameter*``, ``secretsmanager:GetSecretValue``) for the sources you actually use.

To use a different source or a custom config table in a Processor, override the static method
``Processor.get_config(name)`` in your subclass — it is the single point through which the
per-function lookup goes.

Full API reference of the component: :ref:`Config Source <Config_Sourse>`.


Which layer should hold what
----------------------------

* **Structure and safe defaults** → ``DEFAULT_CONFIG``. The code must work with nothing else.
* **Environment-specific values and operational knobs** → the ``config`` table / SSM.
  Everything you may want to change at 3 AM without a deployment.
* **Wiring fixed at build time** → ``custom_config`` via
  ``get_lambda_handler(Processor, global_vars, custom_config=...)``.
* **Secrets** → Secrets Manager (or SSM SecureStrings), never in code or the config table.
