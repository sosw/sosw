.. _Migration to 3.0:

==================================
Migrating to the 3.0 major release
==================================

``sosw`` skipped the 1.x and 2.x lines on purpose: the jump from 0.7.x straight to 3.0 marks a
change of what the package *is*. It began as the *Serverless Orchestrator of Serverless Workers* —
a self-hosted layer that queued, invoked, throttled and retried "Worker" Lambdas. Over the years
the parts everybody actually reused were the foundations underneath: the
:ref:`Processor <Processor>` base class, the :ref:`warm start <Warm Start>` machinery, the
:ref:`components <Components>` and the helpers. Meanwhile AWS shipped managed services that do the
orchestration itself better than a self-hosted queue: Step Functions, EventBridge Scheduler, SQS,
and — since 2025 — durable functions.

**sosw is a framework for bootstrapping AWS Lambda functions.** The self-hosted orchestration
layer is **removed** in the 3.0 major release: it is not deprecated-but-present — it is gone.

If you landed here from an ``AttributeError`` naming a removed entity, this page tells you exactly
what to do.


TL;DR
-----

* Your Lambdas built on ``Processor`` / ``get_lambda_handler`` / ``LambdaGlobals``, the
  components and the helpers: **nothing to do**. That is the product now.
* Your functions built on the orchestration layer (``Orchestrator``, ``Scheduler``, ``Scavenger``,
  ``Worker``...): **pin the previous line** — ``pip install 'sosw<3'`` — and plan the move to
  AWS Step Functions, EventBridge Scheduler or durable functions.
* Read `Behavior changes in this major release`_ — long-standing bugs were fixed and the
  packaging was modernized; the fixes are the only intentional behavior differences in the
  remaining code.


The orchestration layer is removed
----------------------------------

These entities no longer exist in the package:

* ``sosw.Orchestrator``, ``sosw.Scheduler``, ``sosw.Scavenger``,
* ``sosw.Worker``, ``sosw.WorkerAssistant``,
* ``sosw.Labourer``, ``sosw.Essential``,
* the whole ``sosw.managers`` package (``TaskManager``, ``EcologyManager``, ``MetaHandler``).

Accessing a removed name through the package façade (``from sosw import Orchestrator``) raises an
``AttributeError`` that points back to this page. Importing the removed modules directly
(``import sosw.orchestrator``) raises ``ModuleNotFoundError``.

Teams that run the orchestration layer have two options:

#.  **Stay on the 0.7.x line.** ``pip install 'sosw<3'`` keeps everything working exactly as
    before — the 0.7.x releases remain on PyPI, and their documentation is preserved in the
    `previous versions archive <previous/0.7.51/index.html>`__.
#.  **Move to the managed AWS services** (recommended for new designs) — see the mapping below.


What replaces the orchestration layer
-------------------------------------

Pick per use case:

* **Workflows with dependencies, retries, human-visible state** → `AWS Step Functions
  <https://aws.amazon.com/step-functions/>`_. State machines, ``Map`` states for fan-out with
  ``MaxConcurrency`` throttling, built-in retry/catch, execution history (this also covers the
  task audit trail).
* **Cron and delayed invocations** → `Amazon EventBridge Scheduler
  <https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html>`_. Replaces the
  every-minute polling rules and the cron duty of the removed layer.
* **Long-running, checkpointed, single-codebase workflows** → :doc:`durable functions <durable>`
  (``pip install sosw[durable]``) — the spiritual successor of the removed pattern, in-process,
  with per-step retry strategies and checkpoints.
* **Simple queue-based load leveling and retries** → SQS + Lambda event source mappings
  (batching, concurrency controls, DLQs out of the box).

Classes that subclassed the removed entities can usually be rebased on plain
``sosw.app.Processor``: the base lifecycle (config, clients, stats) is the same — only the
task-queue integration calls disappear.


Behavior changes in this major release
--------------------------------------

Beyond the removal, this major release ships exactly these intentional changes:

**1. The explicit ``test`` flag is honored — in both directions.**
    An operator-precedence bug (``kwargs.get('test') or True if ... else False``) made the
    resolver ignore an explicitly passed flag entirely: an explicit ``test=False`` was forced to
    ``True`` in ``test``/``autotest`` stages, **and** an explicit ``test=True`` was forced to
    ``False`` in production stages. Now an explicit flag (the ``test`` kwarg of the Processor, or
    the ``test`` key of the Lambda event) always wins; only when absent is the flag derived from
    ``STAGE``. Audit any code that (accidentally) relied on the old inversion — e.g. passing
    ``test=True`` in production and counting on it being ignored.

**2. ``reset_stats()`` runs once per invocation, not twice.**
    The generated ``lambda_handler`` used to call ``reset_stats()`` twice per invocation
    (non-recursive, then recursive). It now runs exactly once, recursively. Lifetime accumulators
    (``total_*`` and ``lifetime_stats_params``) are preserved as before; if you emitted metrics
    from hooks that observed the intermediate double-reset state, re-verify them.

**3. Packaging: pyproject-only, Python 3.12–3.14.**
    ``setup.py`` is gone; all metadata lives in ``pyproject.toml``. Supported Pythons are
    3.12 – 3.14. ``boto3`` remains the only mandatory runtime dependency, and the new optional
    extra ``sosw[durable]`` pulls the AWS Durable Execution SDK. Build tooling that patched
    ``setup.py`` must target ``pyproject.toml``.

**4. ``import sosw`` is a lazy façade.**
    The package ``__init__`` uses :pep:`562` lazy attribute resolution: ``import sosw`` no longer
    imports ``boto3`` or any submodule, and emits no warnings. ``from sosw import Processor``
    still works — names resolve on first access. Two consequences:

    * import-time side effects are gone — anything that relied on ``import sosw`` transitively
      importing submodules must import them explicitly (``import sosw.app``);
    * ``sosw.__version__`` is now resolved lazily from the installed package metadata.

**5. ``disable_ddb_config`` — the per-function config lookup can be switched off.**
    Set the class attribute ``DISABLE_DDB_CONFIG = True`` on your Processor, or pass a truthy
    ``disable_ddb_config`` key in ``DEFAULT_CONFIG`` / ``custom_config``, to skip the
    ``{AWS_LAMBDA_FUNCTION_NAME}_config`` lookup in DynamoDB / SSM
    (:ref:`Configuration <Configuration>`; absorbed from PR #376).

**6. Config pagination no longer loops forever.**
    ``SecretsManager`` and ``SSMConfig`` pagination helpers never advanced the ``NextToken``
    inside their retry loops: any first response carrying a token re-requested the same page
    indefinitely. Both now advance the token and accumulate every page. If you worked around
    this by keeping secrets or SSM parameters below one page, multi-page results now genuinely
    arrive.

**7. LambdaApi error semantics (for adopters of private forebears).**
    :doc:`LambdaApi <lambda_api>` is new in this release. If you migrate from one of its private
    predecessors, note the hardened contract: authorization runs *before* routing (``401`` even
    for unknown paths), unknown route **or method** → ``404`` (no ``405``), and only
    :py:class:`~sosw.components.exceptions.ApiError` subclasses map to specific HTTP statuses —
    any other exception is logged server-side and rendered as the generic ``500`` envelope
    ``{"error": {"code": "SERVER_ERROR", "message": "Internal server error"}}`` without leaking
    internals. Org-specific pieces (SQLAlchemy session stacks, hardcoded CORS origins and Cognito
    group names) were deliberately not ported — use the config parameters and the
    ``check_route_access`` hook.


Also new in this release
------------------------

* :doc:`LambdaApi <lambda_api>` — declarative router Processor for API Gateway.
* :doc:`Durable functions support <durable>` — ``get_durable_lambda_handler``, ``durable_wait``,
  ``parse_durable_result``; extra ``sosw[durable]``.
* ``helpers.recursive_matches_extract`` gained the ``case_insensitive`` option
  (contributed by `@SHMaryana <https://github.com/SHMaryana>`_, #379).
* The ``ApiError`` exception hierarchy in ``sosw.components.exceptions``
  (:ref:`Exceptions <Exceptions>`).


Upgrade checklist
-----------------

#.  Grep your code for the removed entities (``Orchestrator``, ``Scheduler``, ``Scavenger``,
    ``Worker``, ``WorkerAssistant``, ``Labourer``, ``Essential``, ``sosw.managers``). Any hit →
    either pin ``pip install 'sosw<3'`` or migrate that function to the managed services above.
#.  ``pip install --upgrade sosw`` (or bump the version in your Layer / requirements) on
    Python 3.12+.
#.  Run your test suite. Check that nothing depends on the pre-3.0 ``test``-flag inversion or the
    double ``reset_stats()`` (behavior changes 1–2 above).
#.  If you import submodules through side effects of ``import sosw``, make the imports explicit.
