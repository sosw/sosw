.. _Durable Functions:

=================
Durable functions
=================

`AWS Lambda durable functions <https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html>`_
let a single handler orchestrate long-running workflows (up to a year) with **checkpointed steps**
and compute-free **waits**: on every resume the code replays from the top and completed operations
return their stored results instead of re-executing.

``sosw`` integrates this natively: ``sosw.durable.get_durable_lambda_handler`` is the
durable twin of :py:func:`sosw.app.get_lambda_handler` — the exact same Processor lifecycle and
:ref:`warm start <Warm Start>` behavior, wrapped with the ``durable_execution`` decorator of the
`AWS Durable Execution SDK <https://pypi.org/project/aws-durable-execution-sdk-python/>`_.

..  code-block:: bash

    pip install sosw[durable]

..  important::

    * Every Python version supported by ``sosw`` (3.12 – 3.14) satisfies the durable SDK's own
      minimum Python requirement.
    * The SDK is an *optional* dependency: plain ``import sosw`` / ``import sosw.app`` never
      import it, so regular functions pay zero overhead. Only
      ``get_durable_lambda_handler`` requires it and raises a helpful ``ImportError`` mentioning
      ``pip install sosw[durable]`` when it is missing.
    * The managed AWS Lambda Python runtimes already bundle the SDK, so a deployed function can
      run with an empty ``requirements.txt`` — but AWS recommends pinning the SDK in production
      deployment packages.


Writing a durable Processor
---------------------------

Inside the Processor, ``global_vars.lambda_context`` is the **DurableContext** of the current
invocation: use its ``step()`` / ``wait()`` operations for checkpointed work.

..  code-block:: python

    from sosw.app import LambdaGlobals, Processor as SoswProcessor
    from sosw.durable import get_durable_lambda_handler, durable_step, Duration


    class Processor(SoswProcessor):

        def __call__(self, event, **kwargs):
            super().__call__(event)

            # Checkpointed step: on replay the stored result is returned, the body is skipped.
            data = global_vars.lambda_context.step(self.fetch_data(key=event['key']))

            # Compute-free wait: the execution suspends, you are not billed for the pause.
            global_vars.lambda_context.wait(Duration(seconds=300))

            return global_vars.lambda_context.step(self.publish_report(data=data))


        @durable_step
        def fetch_data(step_context, self, key):
            step_context.logger.info("Fetching %s", key)
            return {'key': key, 'rows': 42}


        @durable_step
        def publish_report(step_context, self, data):
            return {'published': data['key']}


    global_vars = LambdaGlobals()
    lambda_handler = get_durable_lambda_handler(Processor, global_vars)

..  warning::

    Note the parameter order of ``@durable_step`` **methods**: ``(step_context, self, ...)``.
    Accessing ``self.fetch_data`` binds ``self`` as the first *call* argument of the decorator
    wrapper, and at execution time the SDK prepends the live ``StepContext`` — so the original
    function receives ``(step_context, self, *args)``. This is the least obvious part of the
    integration; plain functions decorated with ``@durable_step`` receive just
    ``(step_context, *args)`` as in the official SDK examples.


Enabling durability in SAM
--------------------------

Durability is per-function configuration — ``DurableConfig`` on ``AWS::Serverless::Function``
(or ``AWS::Lambda::Function``):

..  code-block:: yaml

    MyDurableFunction:
      Type: AWS::Serverless::Function
      Properties:
        FunctionName: my-durable-function
        CodeUri: src/
        Handler: app.lambda_handler        # returned by get_durable_lambda_handler
        Runtime: python3.14
        Timeout: 900                       # per-invocation compute timeout
        MemorySize: 512
        DurableConfig:
          ExecutionTimeout: 259200         # whole durable execution, seconds (up to 1 year)
          RetentionPeriodInDays: 1         # checkpoint / history retention
        DeadLetterConfig:                  # recommended: failed async executions are NOT retried
          Type: SQS
          TargetArn: !GetAtt MyDlq.Arn

``ExecutionTimeout`` bounds the whole durable execution (across all replays and waits) and is
independent of the regular ``Timeout``, which still bounds each compute slice.


Invoking durable functions
--------------------------

Durable functions **must be invoked with a qualified ARN** (a version, an alias, or ``$LATEST``):

..  code-block:: python

    response = boto3.client('lambda').invoke(
            FunctionName=f'arn:aws:lambda:{region}:{account}:function:my-durable-function:$LATEST',
            Payload=json.dumps(payload))

An unqualified name fails with ``InvalidParameterValueException``. Pin real version numbers or
aliases in production so that in-flight executions replay against the code that started them.

A synchronous ``invoke()`` returns not your handler result, but an **envelope**:
``{"Status": "SUCCEEDED" | "FAILED" | "PENDING", "Result": "<JSON string>", ...}`` (``PENDING``
when the execution outlives the synchronous call). Unwrap it with
``sosw.durable.parse_durable_result`` — pure Python, no SDK required on the caller side:

..  code-block:: python

    from sosw.durable import parse_durable_result

    payload = json.loads(response['Payload'].read())
    result = parse_durable_result(payload)     # raises RuntimeError on FAILED / PENDING / empty


One codebase, durable and not
-----------------------------

``sosw.durable.durable_wait(seconds)`` waits through the checkpointed ``wait()`` of the
DurableContext when running durable, and falls back to ``time.sleep()`` otherwise — so the same
Processor code can ship as both a durable and a regular Lambda:

..  code-block:: python

    from sosw.durable import durable_wait

    durable_wait(30)    # DurableContext.wait(Duration(seconds=30)) or time.sleep(30)

The fallback path works without the SDK installed.


Operational constraints
-----------------------

Durable execution is checkpoint/replay. The rules below come from the AWS programming model and
from production experience; violating them produces bugs that only show up on replays.

**Code outside steps must be deterministic.**
    On every resume the whole handler re-runs from the top; only durable operations return stored
    results. Never drive control flow outside steps with ``random``, ``time.time()``, ``uuid`` or
    other non-deterministic inputs — or replays will diverge from the original run.

**Step payloads: JSON-serializable, under 256 KB.**
    Step arguments and return values must be JSONifiable. Results under 256 KB are checkpointed
    directly; keep step payloads small and pass references (S3 keys, table keys) instead of blobs.
    A durable execution *result* over the limit may come back empty through a synchronous invoke —
    ``parse_durable_result`` raises ``RuntimeError`` for that case.

**Instance attributes are not replayed.**
    Only step arguments and returns are reliably restored. The Processor is additionally a
    warm-container singleton shared across executions — keep per-execution workflow state in step
    results or external stores (DynamoDB), never on ``self``.

**Never place a durable step after a variable-length wait loop.**
    The SDK identifies operations by their slot in the execution sequence. A polling loop that
    calls ``wait()`` a non-deterministic number of iterations (e.g. zero on the terminal replay)
    shifts the slot counter, and a ``step()`` placed after the loop can land on a slot recorded as
    a *wait* — the SDK returns the cached wait result (``None``) and **silently skips the step
    body**. Run post-loop finalizers inline (idempotent, untracked), or use the SDK's
    ``wait_for_condition`` instead of hand-rolled polling.

**Version carefully.**
    New code versions must tolerate checkpoints written by old ones: do not rename or reorder
    steps of in-flight executions. Deploy new versions/aliases and let old executions drain.

**Configure a DLQ for async invocations.**
    Failed asynchronous durable executions are not retried automatically — attach a
    ``DeadLetterConfig`` and/or EventBridge rules on the FAILED / STOPPED / TIMED_OUT states.

..  note::

    Durable functions are the in-process successor of the self-hosted task queue that the 0.7.x
    line of ``sosw`` shipped — see the :doc:`migration guide <migration_3_0>`.


API reference
-------------

..  automodule:: sosw.durable
    :members:
