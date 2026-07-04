.. _Processor:

=========
Processor
=========

``sosw.app.Processor`` is the core class of the framework. Your Lambda implements a subclass of it
(conventionally called ``Processor`` in your ``app.py``), declares its dependencies and defaults in
``DEFAULT_CONFIG``, and puts the business logic into ``__call__``.

..  code-block:: python

    from sosw.app import LambdaGlobals, get_lambda_handler, Processor as SoswProcessor


    class Processor(SoswProcessor):

        DEFAULT_CONFIG = {
            'init_clients': ['DynamoDb'],
            'dynamo_db_config': {
                'table_name': 'things',
                'row_mapper': {'thing_id': 'S'},
            },
        }


        def __call__(self, event, **kwargs):
            super().__call__(event)
            ...


    global_vars = LambdaGlobals()
    lambda_handler = get_lambda_handler(Processor, global_vars)


Initialization pipeline
-----------------------

``Processor.__init__`` runs the following steps:

#.  Resolve the effective ``test`` flag: an explicitly passed ``test`` keyword always wins,
    otherwise it derives from the ``STAGE`` environment variable (``test`` / ``autotest`` stages
    run in test mode).
#.  Capture the AWS account id from the Lambda context (when available in ``global_vars``).
#.  Assemble ``self.config`` (see `Configuration layering`_ below).
#.  Initialize ``self.stats`` and ``self.result`` counters (``collections.defaultdict(int)``).
#.  Register the clients listed in the ``init_clients`` config parameter
    (see `Client registration`_).


Configuration layering
----------------------

The configuration of a Processor is assembled by ``init_config()`` from three layers, each
recursively updating the previous one:

#.  ``DEFAULT_CONFIG`` — the class attribute of your Processor. Code-level defaults.
#.  The per-function config from DynamoDB / SSM: the record named
    ``{AWS_LAMBDA_FUNCTION_NAME}_config``. This is how you change the behavior of a deployed
    Lambda without redeploying it. Read more: :ref:`Configuration <Configuration>`.
#.  ``custom_config`` — the argument passed to the constructor (usually via
    ``get_lambda_handler(Processor, global_vars, custom_config)``). The final word.

The external lookup of layer 2 can be skipped — for functions that have no ``config`` table, no
permissions to read it, or simply no use for runtime overrides — in either of two ways:

..  code-block:: python

    class Processor(SoswProcessor):
        DISABLE_DDB_CONFIG = True                       # class attribute, or:
        DEFAULT_CONFIG = {'disable_ddb_config': True}   # config flag (works in custom_config too)

``DEFAULT_CONFIG`` and ``custom_config`` are still applied in this case.

..  note:: ``disable_ddb_config`` is new in ``sosw`` 3.0.0 (absorbed from community PR #376).

Anywhere in your code, read configuration values through the ``self._c()`` shortcut with dot
notation and an optional default:

..  code-block:: python

    chunk_size = self._c('processing.chunk_size', 100)


Client registration
-------------------

``register_clients()`` initializes the clients listed in the ``init_clients`` config parameter and
assigns them to the Processor with the ``_client`` suffix. For every name it tries, in order:

#.  a module in the ``components`` or ``managers`` package of *your* Lambda;
#.  a module in ``sosw.components`` or ``sosw.managers`` — names are the CamelCase class stem,
    e.g. ``'DynamoDb'`` → :ref:`DynamoDbClient <DynamoDB_Client>`, ``'Siblings'`` → ``SiblingsManager``;
#.  a plain ``boto3.client()`` — e.g. ``'sts'`` → ``self.sts_client = boto3.client('sts')``.

Class-based clients receive their config from the ``{module_name}_config`` key of the Processor
config (as ``custom_config`` for ``*Manager`` classes and ``config`` for ``*Client`` classes).

For DynamoDB specifically, prefer the lazy factory ``get_ddbc(prefix)``: declare any number of
``{prefix}_dynamo_db_config`` blocks in the config and get fully configured, cached
:ref:`DynamoDbClient <DynamoDB_Client>` instances on demand:

..  code-block:: python

    DEFAULT_CONFIG = {
        'things_dynamo_db_config':  {'table_name': 'things', 'row_mapper': {'thing_id': 'S'}},
        'orders_dynamo_db_config':  {'table_name': 'orders', 'row_mapper': {'order_id': 'S'}},
    }

    def __call__(self, event, **kwargs):
        super().__call__(event)
        things = self.get_ddbc('things').get_by_scan()


The ``stats`` / ``result`` contract
-----------------------------------

Two counters with two different lifetimes:

``self.result``
    Per-invocation state. Reset to an empty ``defaultdict(int)`` at the beginning of every
    ``__call__`` (unless you call ``super().__call__(event, reset_result=False)``). Put anything
    here that describes the outcome of the *current* invocation.

``self.stats``
    Per-container counters. ``__call__`` increments ``processor_calls``; the ``@benchmark``
    decorator adds execution times; your code adds anything else. After every invocation the
    generated handler logs ``get_stats()`` and calls ``reset_stats(recursive=True)`` exactly once:
    ordinary counters are folded into ``total_*`` accumulators which survive for the container
    lifetime, and the parameters listed in the ``lifetime_stats_params`` config are preserved
    as-is.

..  note::

    Up to ``sosw`` 2.x the handler called ``reset_stats()`` twice per invocation; since 3.0.0 it
    is called once (recursively). See the :doc:`migration guide <../migration_3_0>`.

Custom clients participate in both ``get_stats()`` and ``reset_stats()`` recursively when they
implement these methods themselves.


Failing loudly
--------------

``sosw`` code is expected to fail fast and loud. When a Processor cannot continue, call
``self.die(message)``: it logs the message with the current stats, attempts a best-effort SNS
notification to the topic from the ``dead_sns_topic`` config parameter (default
``SoswWorkerErrors``), and terminates the invocation.

The single deliberate exception to the rule is :doc:`LambdaApi <../lambda_api>`, which converts
exceptions into well-formed HTTP error responses — an API must always answer its clients.


API reference
-------------

..  automodule:: sosw.app
    :members:
    :private-members:
