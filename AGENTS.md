# AGENTS.md

Instructions for AI coding agents. Two audiences, one file: agents **building AWS Lambda functions
on top of `sosw`**, and agents **contributing to this repository**. Human docs: https://docs.sosw.app

## What sosw is

Since 3.0.0 `sosw` is a **framework for bootstrapping AWS Lambda functions**: a `Processor` base
class with layered configuration, statistics, and warm-start container reuse, plus thin middleware
components for AWS services. The legacy orchestration layer ("Serverless Orchestrator of Serverless
Workers", pre-3.0) is deprecated: functional through 3.x, warns on instantiation, removed in 4.0.
Do not build anything new on the deprecated entities.

The only mandatory runtime dependency is `boto3`. Supported Python: 3.10–3.14.

## Package map

| Module | Status | One-liner |
|---|---|---|
| `sosw/app.py` | CORE | `Processor` base class, `LambdaGlobals`, `get_lambda_handler` (warm-start caching handler factory) |
| `sosw/lambda_api.py` | CORE | `LambdaApi(Processor)`: declarative router for Lambdas behind API Gateway (REST v1 + HTTP v2), Cognito claims, CORS, JSON envelopes |
| `sosw/durable.py` | CORE, optional dep | `get_durable_lambda_handler` for AWS Lambda durable execution (`pip install sosw[durable]`) + SDK-optional helpers |
| `sosw/components/` | COMPONENT | AWS middleware: `config`, `dynamo_db`, `sns`, `siblings`, `sigv4`, `exceptions` (incl. `ApiError` hierarchy), `benchmark`, `helpers` |
| `sosw/orchestrator.py`, `scheduler.py`, `scavenger.py`, `worker.py`, `worker_assistant.py`, `labourer.py`, `essential.py`, `sosw/managers/` | DEPRECATED | self-hosted orchestration; bugfixes only, removed in 4.0 — prefer Step Functions / EventBridge / durable execution |

Bootstrap tooling: `cookiecutter/` (project template — see its README) and `examples/layers/sosw/`
(Lambda layer build + deploy scripts).

## Build a Lambda on sosw

Import from concrete modules: `from sosw.app import Processor` — not `from sosw import Processor`
(the package init is a lazy façade kept for compatibility).

### Minimal Processor Lambda

```python
"""src/app.py"""

import logging

from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.app import Processor as SoswProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Processor(SoswProcessor):

    DEFAULT_CONFIG = {
        'init_clients': [],     # e.g. ['DynamoDb', 'Sns'] — registered as self.dynamo_db_client, ...
    }


    def __call__(self, event):
        super().__call__(event)     # counts the call, resets self.result

        records = event.get('records', [])
        for record in records:
            self.process_record(record)

        self.result['processed'] = len(records)
        return dict(self.result)


    def process_record(self, record):
        logger.info("Processing record: %s", record)
        self.stats['records_processed'] += 1


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
```

Rules that follow from the warm-start contract:

- `get_lambda_handler` caches the Processor instance in `global_vars` and **reuses it across warm
  invocations** for the container lifetime. Never keep per-invocation state on `self` attributes or
  mutable class attributes (`failed: list = []` is a bug) — it leaks into the next event.
- `self.result` is the per-invocation accumulator — reset on every `__call__` (via
  `super().__call__(event)`).
- `self.stats` is the container-lifetime counter. The handler calls `reset_stats()` after every
  invocation: plain counters are rolled up into `total_*` keys, `total_*` and keys listed in the
  `lifetime_stats_params` config survive as-is.

### Configuration

`Processor.init_config()` builds `self.config` in three layers, each recursively updating the previous:

1. `DEFAULT_CONFIG` class attribute;
2. the per-function config `{AWS_LAMBDA_FUNCTION_NAME}_config` fetched from the DynamoDB `config`
   table (or SSM, depending on the configured source) — this is how you change behavior of a
   deployed Lambda without redeploying;
3. `custom_config` passed to the constructor (usually via `get_lambda_handler(..., custom_config=...)`).

Skip layer 2 (no DDB/SSM lookup, e.g. for Lambdas without such permissions) by setting the class
attribute `DISABLE_DDB_CONFIG = True`, or a truthy `disable_ddb_config` key in `DEFAULT_CONFIG` or
`custom_config`.

Read nested config values with the shortcut `self._c('path.to.param', default)`.

DynamoDB access: declare `{prefix}_dynamo_db_config` in the config and call
`self.get_ddbc('{prefix}')` to lazily get a configured `DynamoDbClient`.

### HTTP API Lambda: LambdaApi

For functions behind API Gateway subclass `sosw.lambda_api.LambdaApi` and declare routes in config:

```python
"""src/app.py"""

from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.components.exceptions import NotFoundError
from sosw.lambda_api import LambdaApi


class Processor(LambdaApi):

    DEFAULT_CONFIG = {
        'auth_enabled': True,       # requires Cognito claims injected by the API Gateway authorizer
        'routes':       {
            'GET /things':            'list_things',
            'GET /things/{thing_id}': 'get_thing',
            'POST /things':           'create_thing',
        },
    }


    def list_things(self, event, **kwargs):
        return {'things': []}


    def get_thing(self, event, thing_id=None, **kwargs):
        if thing_id != '42':
            raise NotFoundError(f"Thing {thing_id} does not exist")
        return {'thing_id': thing_id}


    def create_thing(self, event, **kwargs):
        return self.make_response({'created': True}, status_code=201)


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
```

- Handlers get the raw event plus path parameters as kwargs. Return any JSON-serializable object
  (rendered as `200` with configured CORS headers; `Decimal`/`datetime`/`set` are encoded for you),
  a pre-rendered `{'statusCode': ...}` dict (passed through), or raise an
  `ApiError` subclass from `sosw.components.exceptions` — `BadRequestError` 400,
  `UnauthorizedError` 401, `ForbiddenError` 403, `NotFoundError` 404, `ConflictError` 409,
  `ServerError` 500.
- Auth: the API Gateway authorizer verifies the JWT; `LambdaApi` extracts claims (v1 and v2 shapes)
  into `self.claims`, rejects missing/expired claims with 401, and — when the `authorized_groups`
  config is non-empty — rejects callers outside those Cognito groups with 403. Override
  `check_route_access(route, claims)` for per-route RBAC.
- Config knobs: `auth_enabled`, `authorized_groups`, `cors_headers`, `path_prefixes`.

### Durable functions

For long multi-step workflows on AWS Lambda durable execution mode:

```python
from sosw.durable import get_durable_lambda_handler

global_vars = LambdaGlobals()
lambda_handler = get_durable_lambda_handler(Processor, global_vars)
```

- Requires the optional SDK: `pip install sosw[durable]`. Regular functions keep using
  `get_lambda_handler`, which never imports the SDK.
- Inside the Processor, `global_vars.lambda_context` is the `DurableContext`: use its `step()` /
  `wait()` for checkpointed operations. `sosw.durable.durable_wait(seconds)` works in both worlds
  (checkpointed wait when durable, `time.sleep` otherwise).
- A synchronous `invoke()` of a durable function returns an envelope
  `{"Status": "SUCCEEDED"|"FAILED"|"PENDING", "Result": "<JSON string>"}` — unwrap it on the caller
  side with `sosw.durable.parse_durable_result(payload)` (pure Python, no SDK needed).

### SAM packaging

Ship the function with the `sosw` Lambda layer instead of bundling the package
(see `examples/layers/sosw/`), referencing the layer ARN from SSM:

```yaml
Parameters:
  LambdaLayerSoswLatestArn:
    Type: AWS::SSM::Parameter::Value<String>
    Default: lambda-layer-sosw-latest

Resources:
  MyFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: src/
      Handler: app.lambda_handler
      Layers:
        - !Ref LambdaLayerSoswLatestArn
```

Or scaffold the whole project: `cookiecutter https://github.com/sosw/sosw --directory cookiecutter`.

## Test

### Pattern for Lambdas built on sosw

Unit tests are pure: no network, no real AWS. Set the environment **before importing** anything
that imports sosw, patch `boto3.client`, and patch the config lookup:

```python
"""test/test_unit.py"""

import os
import sys
import unittest

from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

os.environ['STAGE'] = 'test'
os.environ['autotest'] = 'True'

from app import Processor


class ProcessorTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher_boto = patch('boto3.client')
        self.mock_boto_client = self.patcher_boto.start()
        self.mock_boto_client.return_value = MagicMock()

        with patch.object(Processor, 'get_config', return_value={}):
            self.processor = Processor(test=True)


    def tearDown(self):
        self.patcher_boto.stop()
        del self.processor


    def test_call(self):
        result = self.processor({'records': ['a', 'b']})
        self.assertEqual(result['processed'], 2)
```

**Always add the container-reuse regression test.** Lambda containers are warm-reused, so the same
Processor instance serves consecutive events; state leaks are the classic sosw bug. Invoke twice
and assert the second result reflects only the second event:

```python
    def test_no_state_leak_between_invocations(self):
        """Simulate a warm container: second call must not see first call's state."""
        self.processor({'records': ['a', 'b', 'c']})
        result = self.processor({'records': ['d']})

        self.assertEqual(result['processed'], 1)    # 1, not 4 accumulated
```

### Contributing tests to sosw itself

- `unittest.TestCase` style. Files live in `sosw/test/unit/`, `sosw/components/test/unit/`,
  `sosw/managers/test/unit/`.
- **Register every new unit test file in `sosw/test/suite_unit.py`** (import the TestCase and
  `addTest` it) — CI runs only that explicit suite, pytest discovery is not used. An unregistered
  test never runs.
- Run the suite: `python -m pytest sosw/test/suite_unit.py -v`. Never run pytest over the whole
  tree — legacy `test/integration/` files create real AWS resources.
- Coverage is enforced at **100%** of `sosw/` (test dirs excluded via `.coveragerc`):
  `python -m pytest sosw/test/suite_unit.py --cov=sosw --cov-report=term-missing`.
  Cover optional-import guards (e.g. powertools) by reloading the module with a patched
  `sys.modules`, not with `# pragma: no cover`.
- Keep the suite fast (seconds) and network-free: `patch('boto3.client')`, `MagicMock()` clients,
  `patch.object(Processor, 'get_config', return_value={...})`.

## Debug

- **Bump log verbosity per invocation**: pass `"logging_level": "DEBUG"` in the Lambda event —
  the generated handler calls `logger.setLevel()` with it before processing.
- **Contracts**: the handler returns whatever your `__call__` returns and logs `get_stats()` after
  every invocation. Business output belongs in `self.result`; metrics/counters in `self.stats`.
- Common failure modes:
  - *Config silently empty*: if `{AWS_LAMBDA_FUNCTION_NAME}_config` does not exist in the DynamoDB
    `config` table (or the function lacks read permissions), `get_config` returns `{}` — the
    Processor proceeds with only `DEFAULT_CONFIG` + `custom_config`. If DDB config is intended,
    verify the record name matches the deployed function name exactly.
  - *`ValueError: get_ddbc() method supports only prefixes: [...]`*: you called
    `self.get_ddbc('x')` but the final config has no `x_dynamo_db_config` key. Check which config
    layer was supposed to deliver it.
  - *LambdaApi never raises to the caller*: `LambdaApi.__call__` converts everything to an HTTP
    response — `ApiError` subclasses to their status envelope
    `{'error': {'code': ..., 'message': ...}}`, unexpected exceptions to a generic 500 (logged
    server-side with full traceback, counted in `stats['api_errors_5xx']`). A 401/403 never
    includes claim contents; look at the CloudWatch logs for the reason. Unknown route or method
    is 404.
  - *Durable result looks wrapped*: synchronous invokes of durable functions return the
    `{"Status", "Result"}` envelope; unwrap with `parse_durable_result` — it raises `RuntimeError`
    on `FAILED`/`PENDING`/empty results instead of handing you the envelope.
  - *State from the previous event*: warm container reuse (see the regression test above).

## Style rules for contributions

PRs violating these get bounced in review:

- PEP-8 with **two blank lines** between functions/methods/classes; vertically aligned dict values.
- **Single quotes** for regular strings and dict keys; **double quotes** for logging and exception
  messages.
- Logging via `%` formatting: `logger.info("Processing %s", thing)` — **never f-strings in logger
  calls**.
- Imports at the top of the module, grouped in order: full core imports, full custom imports,
  partial core imports (`from x import y`), partial custom imports.
- **Fail fast and loud**: no `try/except: pass` around business logic; Lambdas must raise on errors
  (the only sanctioned exception-to-response conversion is the `LambdaApi` HTTP contract layer).
- snake_case data fields in events/payloads/records.
- reST docstrings (`:param x:`, `:rtype:`) on public callables; new `sosw` modules start with the
  MIT license header docstring (copy it from `sosw/app.py`).
- Every file ends with exactly one trailing newline.

## Repo mechanics

- Branch flow: feature branch → PR into the current **`X_Y_Z` staging branch** (e.g. `3_0_1`), not
  into `master`. Pushes to staging branches publish release candidates to TestPyPI; merging the
  release PR `X_Y_Z → master` publishes to PyPI. Never merge to `master` yourself.
- CI gates on PRs: unit suite on Python 3.10–3.14, **100% coverage**, and a Sphinx docs build with
  `-W` (any docs warning fails the build — update `docs/` in the same PR when you change public
  API or docstrings referenced there).
- Deprecated modules: bugfixes only, no new features, nothing removed before 4.0.
- `examples/`, `cookiecutter/`, and `AGENTS.md` must track the public API: if your change alters
  signatures or config conventions shown there, update them in the same PR.
