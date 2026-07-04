.. _Migration to 3.0:

======================
Migrating to sosw 3.0
======================

``sosw`` 3.0.0 repurposes the package. It began as the *Serverless Orchestrator of Serverless
Workers* — a self-hosted layer that queued, invoked, throttled and retried "Worker" Lambdas.
Over the years the parts everybody actually reused were the foundations underneath: the
:ref:`Processor <Processor>` base class, the :ref:`warm start <Warm Start>` machinery, the
:ref:`components <Components>` and the helpers. Meanwhile AWS shipped managed services that do the
orchestration itself better than a self-hosted queue: Step Functions, EventBridge Scheduler, SQS,
and — since 2025 — durable functions.

**sosw 3.0 is a framework for bootstrapping AWS Lambda functions.** The orchestration layer is
deprecated: it stays fully functional through every 3.x release (no behavior change beyond a
warning) and will be **removed in 4.0**.

If you landed here from a ``DeprecationWarning``, this page tells you exactly what to do.


TL;DR
-----

* Your Lambdas built on ``Processor`` / ``get_lambda_handler`` / ``LambdaGlobals``, the
  components and the helpers: **nothing to do**. That is the product now.
* Your ``Worker`` subclasses keep working, but plan to rebase them on plain ``Processor``.
* Your ``Orchestrator`` / ``Scheduler`` / ``Scavenger`` Essentials keep working through 3.x,
  but plan the move to Step Functions, EventBridge Scheduler or durable functions before 4.0.
* Read `Behavior changes in 3.0.0`_ — three long-standing bugs were fixed and the packaging was
  modernized; the fixes are the only intentional behavior differences.


What is deprecated
------------------

Instantiating any of the following emits a ``DeprecationWarning`` (once per entity per process).
Importing them stays silent, and ``import sosw`` imports nothing heavy at all.

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Deprecated entity
     - Migration guidance
   * - ``sosw.Orchestrator``
     - Invocation fan-out and throttling of workers → AWS Step Functions (``Map`` states with
       ``MaxConcurrency``), or EventBridge → SQS with Lambda event-source scaling controls.
   * - ``sosw.Scheduler``
     - Cron-style business jobs and payload chunking → EventBridge Scheduler for the timing;
       chunk inside a Step Functions ``Map`` / ``Distributed Map`` state, or in a durable
       function loop.
   * - ``sosw.Scavenger``
     - Retry / dead-letter handling → native Lambda async retries + DLQs (SQS
       ``DeadLetterConfig``), Step Functions ``Retry`` / ``Catch`` blocks, or durable-function
       retry strategies per step.
   * - ``sosw.Worker``
     - Rebase the class on plain ``sosw.app.Processor``. The only things ``Worker`` added on top
       are task-closing calls to the orchestration layer (``mark_task_as_completed`` /
       WorkerAssistant integration) — drop them together with the orchestration.
   * - ``sosw.WorkerAssistant``
     - Disappears together with the task queue. Completion signaling → Step Functions task
       states / callbacks (``SendTaskSuccess``) or durable-function checkpoints.
   * - ``sosw.Labourer``
     - Internal abstraction of a registered Worker. No replacement needed — its attributes
       (ARN, concurrency limits, health metrics) map onto Step Functions / EventBridge
       configuration.
   * - ``sosw.Essential``
     - The base class of the orchestration Lambdas themselves. Rebase anything of yours on
       ``sosw.app.Processor``.
   * - ``sosw.managers.task.TaskManager``
     - The DynamoDB task queue (``sosw_tasks`` tables, greenfield timestamps). Replace with Step
       Functions executions, SQS queues, or durable-function state — all managed, all with
       built-in visibility.
   * - ``sosw.managers.ecology.EcologyManager``
     - Health-based throttling of workers → CloudWatch alarms driving Step Functions
       concurrency, Application Auto Scaling, or circuit-breaker steps in a durable function.
   * - ``sosw.managers.meta_handler.MetaHandler``
     - Task audit trail → Step Functions execution history, CloudWatch Logs / X-Ray traces, or
       your own DynamoDB journal written from steps.

The preserved documentation of the deprecated layer lives under
:doc:`Deprecated: orchestration <deprecated/index>`.


What replaced orchestration
---------------------------

Pick per use case:

* **Workflows with dependencies, retries, human-visible state** → `AWS Step Functions
  <https://aws.amazon.com/step-functions/>`_. State machines, ``Map`` states for fan-out,
  built-in retry/catch, execution history.
* **Cron and delayed invocations** → `Amazon EventBridge Scheduler
  <https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html>`_. Replaces the
  every-minute Orchestrator/Scavenger rules and the Scheduler's cron duty.
* **Long-running, checkpointed, single-codebase workflows** → :doc:`durable functions <durable>`
  (``pip install sosw[durable]``) — the spiritual successor of the Worker pattern, in-process.
* **Simple queue-based load leveling** → SQS + Lambda event source mappings (batching,
  concurrency controls, DLQs out of the box).


Behavior changes in 3.0.0
-------------------------

Beyond the deprecation warnings, 3.0.0 ships exactly these intentional changes:

**1. The explicit ``test`` flag is honored — in both directions.**
    An operator-precedence bug (``kwargs.get('test') or True if ... else False``) made the
    resolver ignore an explicitly passed flag entirely: an explicit ``test=False`` was forced to
    ``True`` in ``test``/``autotest`` stages, **and** an explicit ``test=True`` was forced to
    ``False`` in production stages. Since 3.0.0 an explicit flag (the ``test`` kwarg of the
    Processor, or the ``test`` key of the Lambda event) always wins; only when absent is the flag
    derived from ``STAGE``. Audit any code that (accidentally) relied on the old inversion —
    e.g. passing ``test=True`` in production and counting on it being ignored.

**2. ``reset_stats()`` runs once per invocation, not twice.**
    The generated ``lambda_handler`` used to call ``reset_stats()`` twice per invocation
    (non-recursive, then recursive). It now runs exactly once, recursively. Lifetime accumulators
    (``total_*`` and ``lifetime_stats_params``) are preserved as before; if you emitted metrics
    from hooks that observed the intermediate double-reset state, re-verify them.

**3. Packaging: pyproject-only, Python 3.10–3.14.**
    ``setup.py`` is gone; all metadata lives in ``pyproject.toml``. Supported Pythons are
    3.10 – 3.14. ``boto3`` remains the only mandatory runtime dependency, and the new optional
    extra ``sosw[durable]`` pulls the AWS Durable Execution SDK (which itself needs Python
    >= 3.11). Build tooling that patched ``setup.py`` must target ``pyproject.toml``.

**4. ``import sosw`` is a lazy façade.**
    The package ``__init__`` uses :pep:`562` lazy attribute resolution: ``import sosw`` no longer
    imports ``boto3``, any submodule, or the orchestration layer, and emits no warnings.
    ``from sosw import Processor`` and even ``from sosw import Orchestrator`` still work — names
    resolve on first access (deprecated classes warn when *instantiated*). Two consequences:

    * import-time side effects are gone — anything that relied on ``import sosw`` transitively
      importing submodules must import them explicitly (``import sosw.app``);
    * ``sosw.__version__`` is now resolved lazily from the installed package metadata.

**5. LambdaApi error semantics (for adopters of private forebears).**
    :doc:`LambdaApi <lambda_api>` is new in 3.0.0. If you migrate from one of its private
    predecessors, note the hardened contract: authorization runs *before* routing (``401`` even
    for unknown paths), unknown route **or method** → ``404`` (no ``405``), and only
    :py:class:`~sosw.components.exceptions.ApiError` subclasses map to specific HTTP statuses —
    any other exception is logged server-side and rendered as the generic ``500`` envelope
    ``{"error": {"code": "SERVER_ERROR", "message": "Internal server error"}}`` without leaking
    internals. Org-specific pieces (SQLAlchemy session stacks, hardcoded CORS origins and Cognito
    group names) were deliberately not ported — use the config parameters and the
    ``check_route_access`` hook.


Also new in 3.0
---------------

* :doc:`LambdaApi <lambda_api>` — declarative router Processor for API Gateway.
* :doc:`Durable functions support <durable>` — ``get_durable_lambda_handler``, ``durable_wait``,
  ``parse_durable_result``; extra ``sosw[durable]``.
* ``disable_ddb_config`` — skip the per-function DynamoDB/SSM config lookup
  (:ref:`Configuration <Configuration>`; absorbed from PR #376).
* ``helpers.recursive_matches_extract`` gained the ``case_insensitive`` option
  (contributed by `@SHMaryana <https://github.com/SHMaryana>`_, #379).
* The ``ApiError`` exception hierarchy in ``sosw.components.exceptions``
  (:ref:`Exceptions <Exceptions>`).


Upgrade checklist
-----------------

#.  ``pip install --upgrade sosw`` (or bump the version in your Layer / requirements).
#.  Run your test suite with warnings visible: ``python -W default::DeprecationWarning -m pytest``.
    Every warning names the deprecated entity and points back to this page.
#.  Check that nothing depends on the pre-3.0 ``test``-flag inversion or the double
    ``reset_stats()`` (behavior changes 1–2 above).
#.  If you import submodules through side effects of ``import sosw``, make the imports explicit.
#.  Plan the retirement of your orchestration Essentials before 4.0 — the deprecated layer keeps
    working through 3.x, so this can be a calm, gradual migration.
