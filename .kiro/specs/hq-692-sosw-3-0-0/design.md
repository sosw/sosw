# sosw 3.0.0 — Design

## 1. Module disposition

| Module | 3.0 status | Notes |
|---|---|---|
| `sosw/app.py` | CORE | `Processor`, `LambdaGlobals`, `get_lambda_handler`; gains shared handler builder + warm-start fixes |
| `sosw/lambda_api.py` | NEW CORE | `LambdaApi(Processor)` declarative API Gateway base |
| `sosw/durable.py` | NEW CORE | `get_durable_lambda_handler` + SDK-optional helpers |
| `sosw/components/*` (config, dynamo_db, helpers, sns, siblings, sigv4, exceptions, decorators, benchmark) | COMPONENT | kept, tidied, 100% covered; `exceptions` gains `ApiError` hierarchy |
| `sosw/orchestrator.py`, `scavenger.py`, `scheduler.py`, `worker.py`, `worker_assistant.py`, `labourer.py`, `essential.py`, `managers/*` | REMOVED | deleted in 3.0 (re-baselined during release review from the earlier deprecate-and-keep plan); users pin `sosw<3` |

## 2. Removal mechanics

*(Re-baselined during release review — the initially designed DeprecationWarning approach shipped
first and was then superseded by full removal in the same release cycle.)*

- The orchestration modules, their tests, fixtures, examples and docs sections are deleted.
- `sosw/__init__.py` stays a **lazy façade** via module `__getattr__` (PEP 562): `import sosw` imports
  nothing heavy; `sosw.Processor`, `sosw.LambdaApi` resolve lazily. The seven removed public names
  raise `AttributeError` with guidance (pin `sosw<3`, migration guide link).
- `siblings.py` imports `Processor` from `sosw.app` directly (no façade round-trip).
- The docs migration page explains the removal, maps orchestration to AWS-native services
  (Step Functions / EventBridge / durable functions), and links the preserved 0.7.x docs archive
  (`previous/0.7.51/`, compiled artifacts committed under `previous_versions/`).

## 3. Warm start

Behavior-preserving refactor of `get_lambda_handler` into a shared `_make_lambda_handler(...)`
used by both the classic and durable handler factories. Fixes riding along:

- `test` flag precedence: `kwargs.get('test') or True if os.environ.get('STAGE') in ['test', 'autotest'] else False`
  parses as `A or (B if C else D)`; rewritten to explicit logic honoring an explicitly passed flag.
- Single `reset_stats()` per invocation (currently called twice, non-recursive then recursive).
- `disable_ddb_config` config/class flag to skip per-function DDB/SSM config lookup (PR #376 absorbed).
- Processor caching contract documented: one Processor per container via `LambdaGlobals`; per-invocation
  state lives in `self.result` (reset each call), lifetime accumulators in `self.stats` (`total_*` preserved).

## 4. lambda_api

Top-level `sosw/lambda_api.py` (a peer of the handler factories, not a `components/` client).

- **Router**: config `routes = {'GET /things': 'get_things', ...}` mapping to bound method names;
  supports API Gateway REST (v1) and HTTP API (v2) event shapes; path parameters via APIGW resource syntax.
- **Auth**: Cognito JWT claims already verified by the API Gateway authorizer; LambdaApi extracts claims
  from both `requestContext.authorizer.claims` (v1) and `requestContext.authorizer.jwt.claims` (v2),
  tolerates numeric/string `exp`, exposes `self.claims`; optional `authorized_groups` config gate and a
  `check_route_access(route, claims)` hook for downstream RBAC.
- **Responses**: `make_response(body, status_code=200, headers=None)` with config-driven CORS headers,
  JSON encoder handling `Decimal`/`datetime`/`set` (DynamoDB-friendly), passthrough for pre-rendered
  `{'statusCode': ...}` dicts.
- **Errors**: `ApiError(Exception)` hierarchy in `sosw.components.exceptions` —
  `BadRequestError(400)`, `UnauthorizedError(401)`, `ForbiddenError(403)`, `NotFoundError(404)`,
  `ConflictError(409)`, `ServerError(500)`. `__call__` catches `ApiError` → envelope
  `{'error': {'code', 'message'}}`; unexpected exceptions → logged with stack, generic 500 envelope.
- **Stats**: per-route counters through `self.stats`.
- Explicitly NOT ported from private forebears: SQLAlchemy session stack, org-specific permission
  modules, hardcoded CORS origins, hardcoded Cognito group names.

## 5. Durable functions

- `sosw/durable.py`: `get_durable_lambda_handler(processor_class, global_vars=None, custom_config=None, **durable_kwargs)`
  → wraps the shared handler with the durable-execution SDK decorator. Import of the SDK guarded;
  missing SDK raises `ImportError("... pip install sosw[durable]")` at module import.
- Helpers usable with or without the SDK: `durable_wait(seconds)` (SDK wait or `time.sleep` fallback),
  `parse_durable_result(payload)` (unwraps `{"Status": ..., "Result": ...}` sync-invoke envelopes).
- `sosw.app` keeps zero knowledge of the SDK.
- Docs carry the operational constraints: determinism requirements, 256KB step payload limit, qualified
  ARN invocation, and the step-after-variable-wait-loop hazard.
- Packaging: `[project.optional-dependencies] durable = ["aws-durable-execution-sdk-python"]`.

## 6. Tests & coverage

- Suite stays `unittest`-style, registered explicitly in `sosw/test/suite_unit.py`; CI runs only that suite.
- boto3 fully mocked (`patch('boto3.client')`, `patch.object(Processor, 'get_config')`); no moto, no network.
- Optional-powertools import guards covered by reloading modules under a patched `sys.modules`
  (both `ImportError` and success branches) — no `# pragma: no cover`.
- Orphan test files (`test_essential`, unit `test_secrets_manager`, `test_dynamo_db_init`) registered;
  duplicate legacy `components/test/test_config.py` retired in favor of the unit variant.
- Coverage gate: `--cov=sosw --cov-fail-under=100` in CI (test dirs excluded via `.coveragerc`).

## 7. CI/CD

- `run-unittests.yml`: matrix 3.12/3.13/3.14 → pytest suite; separate `coverage` job (3.14)
  with `--cov-fail-under`.
- `docs-builder-action.yaml`: pip-based install (no pipenv), `sphinx-build -W`.
- `publish-to-test-pypi.yml`: RC trigger pattern covers `[0-9]+_[0-9]+_[0-9]+` branches; version patch
  step updated for `pyproject.toml`.
- `publish-to-pypi.yml`: unchanged semantics (master push → PyPI); version now read from pyproject.
- README badges: GitHub Actions (tests, docs), PyPI version/downloads/license.

## 8. Docs architecture

- Sphinx + `furo` theme (modern, dark-mode, mobile-friendly; replaces RTD theme), strict `-W` builds.
- Structure: `index` (framework pitch) → `quickstart` → `concepts/` (processor, config, stats,
  warm-start) → `components/` → `lambda_api` → `durable` → `tutorials/` → `migration_3_0` →
  `contribution/`; legacy orchestration docs are NOT part of the current docs — the compiled 0.7.x
  site is preserved verbatim under `previous/0.7.51/` (re-baselined during release review).
- `AGENTS.md` at repo root for coding agents; `.kiro/steering/{product,tech,structure}.md` for
  spec-driven sessions.

## 9. Delivery pipeline

Feature branches off `3_0_0`, PR per work stream into `3_0_0` (see tasks.md), each PR:
implementation (Joshua) → code review posted on the PR (Arkady, via bender-sosw) → merge (ngr commits,
bender-sosw approval). Final `3_0_0 → master` PR is opened but **left for human approval** — merging it
publishes 3.0.0 to PyPI.
