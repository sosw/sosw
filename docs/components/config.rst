.. _Config_Sourse:

Config Source
-------------

The configuration-source component behind the automatic per-function config lookup of every
:ref:`Processor <Processor>`. For the concepts — layering, the ``config`` table schema, when to
use which source — see :ref:`Configuration <Configuration>`. This page is the API reference.

..  automodule:: sosw.components.config
    :members:

..  autoclass:: sosw.components.config.DynamoConfig
    :members:

..  autoclass:: sosw.components.config.SecretsManager
    :members:

..  autoclass:: sosw.components.config.SSMConfig
    :members:
