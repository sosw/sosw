.. _Warm Start:

==========
Warm start
==========

AWS Lambda reuses execution environments ("containers") between invocations. Everything
initialized *outside* the handler survives to the next call — and Processor initialization
(config assembly, possibly a DynamoDB lookup, client construction) is exactly the kind of work
you want to pay for once per container, not once per request.

``sosw`` packages this pattern into two small pieces:

..  code-block:: python

    from sosw.app import LambdaGlobals, get_lambda_handler


    class Processor(SoswProcessor):
        ...


    global_vars = LambdaGlobals()
    lambda_handler = get_lambda_handler(Processor, global_vars)


What the generated handler does
-------------------------------

On every invocation, the ``lambda_handler`` returned by :py:func:`sosw.app.get_lambda_handler`:

#.  Optionally raises the logging level from the ``logging_level`` key of the event.
#.  Resolves the effective ``test`` flag from the ``test`` key of the event
    (an explicit flag always wins; otherwise derived from the ``STAGE`` environment variable).
#.  Stores the fresh Lambda ``context`` in ``global_vars.lambda_context`` — the context is unique
    per invocation, even though the Processor is not.
#.  Constructs the Processor **only if** ``global_vars.processor`` is ``None`` — i.e. once per
    container. Warm invocations reuse the cached instance.
#.  Calls the Processor with the event and captures the result.
#.  Logs ``get_stats()`` and calls ``reset_stats(recursive=True)`` — exactly once.
#.  Returns the result.


The contract: one Processor per container
------------------------------------------

``LambdaGlobals`` is a tiny namespace that survives for the lifetime of the Lambda container.
It holds two things:

* ``global_vars.processor`` — the cached Processor instance;
* ``global_vars.lambda_context`` — the context of the *current* invocation (reset by the handler
  every call).

This gives you a simple set of rules to write correct warm-start code:

* **Per-invocation state** belongs in ``self.result`` (reset every call), local variables, or the
  event itself. Never leave request-scoped data in instance attributes: the next invocation of the
  warm container will see it.
* **Container-lifetime state** — clients, caches, compiled regexes, the assembled config — belongs
  on the Processor instance and is the whole point of the pattern.
* **Counters** follow the :ref:`stats / result contract <Processor>`: ``self.stats`` values are
  folded into ``total_*`` accumulators after every invocation.
* **The Lambda context** must always be read through ``global_vars.lambda_context``, never cached:
  each invocation brings a new one (remaining time, request id...).

..  warning::

    ``LambdaGlobals`` instances proxy module-level globals of ``sosw.app``: constructing a new
    ``LambdaGlobals()`` resets the shared ``lambda_context`` placeholder. Create exactly one
    instance, at the module level of your ``app.py``, and pass it to ``get_lambda_handler``.


The ``test`` flag
-----------------

Both the Processor constructor and the generated handler resolve the effective test mode with the
same rule (:py:func:`sosw.app._derive_test_flag`):

* an **explicitly provided flag always wins** — ``test=False`` passed in a test stage stays
  ``False``, ``test=True`` passed in production stays ``True``;
* when no flag is provided, it derives from the environment: ``STAGE`` set to ``test`` or
  ``autotest`` means test mode.

..  note::

    Up to ``sosw`` 2.x an operator-precedence bug inverted this rule in both directions (an
    explicit flag was ignored whenever the ``STAGE`` said otherwise). Fixed in 3.0.0 — see the
    :doc:`migration guide <../migration_3_0>`.


Cold starts still exist
-----------------------

The first invocation of every container pays the full price: package import plus Processor
construction. To keep it low:

* keep your deployment package small — mount ``sosw`` from a shared Lambda Layer
  (:ref:`SOSW Layer`);
* skip the external config lookup when you do not need it
  (``disable_ddb_config`` — see :ref:`Configuration <Configuration>`);
* initialize only the clients you actually use (``init_clients``), and use lazy factories like
  ``get_ddbc()`` for the rest.
