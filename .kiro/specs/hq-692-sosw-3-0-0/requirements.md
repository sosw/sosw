# sosw 3.0.0 — Requirements

Tracker: SOSW HQ ticket HQ-692 · Target branch: `3_0_0` · Released from `master` merge (auto-publishes to PyPI).

## Vision

sosw 3.0.0 repurposes the package from "Serverless Orchestrator of Serverless Workers" into a
**framework for bootstrapping AWS Lambda functions**: the `Processor` base class (with warm-start
container reuse), the AWS middleware components, and the helpers are the product. The legacy
orchestration layer stays importable but is formally deprecated.

## R1 — Packaging & version

- WHEN the package is built, THE SYSTEM SHALL read all metadata from `pyproject.toml` (setup.py removed or reduced to a shim) with `version = "3.0.0"`.
- THE SYSTEM SHALL declare support for Python 3.10, 3.11, 3.12, 3.13 and 3.14 in classifiers and `requires-python >= 3.10`.
- THE SYSTEM SHALL keep `boto3` as the only mandatory runtime dependency.
- THE SYSTEM SHALL expose optional extras: `sosw[durable]` for the AWS durable execution SDK.
- WHEN dev dependencies are locked, THE SYSTEM SHALL NOT ship known-vulnerable pins (current Pipfile.lock carries 15 Dependabot alerts; regenerate or remove the lockfile).

## R2 — Deprecation of the orchestration layer

- WHEN a user imports or instantiates `Orchestrator`, `Scavenger`, `Scheduler`, `Worker`, `WorkerAssistant`, `Labourer`, `Essential`, `managers.task.TaskManager`, `managers.ecology.EcologyManager`, or `managers.meta_handler.MetaHandler`, THE SYSTEM SHALL emit a `DeprecationWarning` naming the replacement guidance and the removal horizon (4.0), exactly once per class per process.
- THE SYSTEM SHALL keep every deprecated entity fully functional in 3.x (no behavior change beyond the warning).
- `sosw/__init__.py` SHALL become a lazy façade (PEP 562 `__getattr__`) so that `import sosw` does not import orchestration modules or emit warnings, while `from sosw import Orchestrator` still works (with warning).
- Internal cross-imports (`siblings.py`, `ecology → task`) SHALL NOT trigger the deprecation warnings during normal component use.

## R3 — Processor & warm start (core)

- `Processor`, `LambdaGlobals` and `get_lambda_handler` remain the core public API; `get_lambda_handler` SHALL keep caching the Processor instance across warm invocations.
- THE SYSTEM SHALL fix the `test` flag operator-precedence bug (`kwargs.get('test') or True if ...`) so an explicit `test=False`/absent flag is honored outside test stages.
- THE SYSTEM SHALL stop double-calling `reset_stats()` per invocation in the generated handler.
- WHEN `custom_config` contains `disable_ddb_config: True` (or the Processor attribute `DISABLE_DDB_CONFIG` is set), THE SYSTEM SHALL skip the per-function DynamoDB/SSM config lookup (absorbed from PR #376).

## R4 — lambda_api component

- THE SYSTEM SHALL provide `sosw.lambda_api.LambdaApi(Processor)`: a declarative-router base class for Lambdas behind API Gateway (REST v1 and HTTP v2 payloads).
- It SHALL support: route table config (`method + path → handler`), Cognito JWT claims extraction (id & access token shapes, numeric and string `exp`), configurable authorization groups, config-driven CORS headers, JSON response envelope with a DynamoDB-aware encoder (Decimal/datetime/set), pre-rendered response passthrough, and an `ApiError` exception hierarchy (400/401/403/404/409/500) in `sosw.components.exceptions`.
- Auth failures SHALL return 401/403 without leaking claim contents; unknown routes 404; handler exceptions 500 with a machine-readable error envelope and full server-side logging.
- The component SHALL be fully unit-tested with mocked events (no network), covering the documented test matrix.

## R5 — Durable functions support

- THE SYSTEM SHALL provide `sosw.durable.get_durable_lambda_handler(...)` producing a handler wrapped with the AWS durable execution SDK, reusing the exact non-durable handler behavior (extracted shared builder in `sosw.app`).
- `import sosw` and `import sosw.app` SHALL NOT import the durable SDK; `sosw.durable` SHALL raise a helpful `ImportError` mentioning `pip install sosw[durable]` when the SDK is missing.
- SDK-optional helpers SHALL be provided: `durable_wait(seconds)` (falls back to `time.sleep`) and `parse_durable_result(payload)` (unwraps the `{"Status","Result"}` invocation envelope).
- Unit tests SHALL run with the SDK absent (fake-SDK injection) and assert no SDK leakage into `sosw.app`.

## R6 — Components & helpers tidy-up

- `recursive_matches_extract` SHALL gain the `case_insensitive` option (community PR #379, with credit).
- `scheduler.py` regex SHALL use a raw string (kills the Python 3.12+ SyntaxWarning).
- Helpers keep full backward compatibility; docstrings completed where missing.

## R7 — Tests & coverage

- THE SYSTEM SHALL reach and enforce **100% unit-test line coverage** of `sosw/` (test dirs excluded per `.coveragerc`), including both branches of the optional-powertools import guards.
- All existing orphan test files SHALL be registered in the unit suite or removed; the legacy `components/test/test_config.py` (creates real DynamoDB tables) SHALL stop shadowing the unit variant.
- The unit suite SHALL stay network-free and complete in well under a minute.

## R8 — CI (GitHub Workflows only)

- PR checks SHALL run: unit tests on a 3.10–3.14 matrix, a coverage job failing under the enforced threshold (100 at release), and a docs build (`sphinx -W`).
- The TestPyPI RC publish SHALL trigger for `X_Y_Z`-style staging branches (not only `0_*`) and patch the version wherever it lives after the pyproject migration.
- Travis badges/references SHALL be removed from README and docs; badges point at GitHub Actions.
- The stray repo-root `.aws/config` SHALL be removed; `.claude/` ignored.

## R9 — Documentation (humans)

- Docs SHALL be rebuilt Sphinx-first with a modern theme, restructured as: quickstart (pip install → first Processor Lambda in minutes), framework concepts (Processor, config, stats, warm start), per-component guides, lambda_api guide, durable guide, tutorials, contribution guide, and a 0.x → 3.0 migration page (orchestration deprecation front and center).
- Every documented example SHALL match the 3.0 API.

## R10 — Documentation (AI agents)

- The repo SHALL ship `AGENTS.md` (GitHub issue #388) written for coding agents: package map, how to implement a Lambda on sosw, how to test (mock patterns), how to debug, style rules.
- `.kiro/steering/` SHALL contain product/tech/structure steering docs for spec-driven work in this repo.

## R11 — Bootstrap tooling

- A cookiecutter template SHALL scaffold a new sosw-based Lambda (src/app.py on Processor, unit tests, template.yaml, samconfig.toml, README) with documented usage.
- A Lambda layer example SHALL ship with autobuild + autodeploy scripts, bundling sosw and optionally (default-on) aws-lambda-powertools and aws-xray-sdk.

## R12 — Community hygiene

- Every open GitHub issue and PR SHALL receive a triage comment from the maintainer account (bender-sosw); obsolete ones closed with reasoning, keepers tagged to the 3.x roadmap.
- New improvement issues discovered during this release SHALL be filed (typing/mypy, moto-based DDB tests, async story, powertools matrix, etc.).

## Out of scope for 3.0.0

- Removing any deprecated entity (that is 4.0).
- Async/await support, AthenaManager, and other KEEP-ROADMAP issues (filed/kept for 3.x).
- Publishing to PyPI from this ticket: happens automatically when a human merges `3_0_0 → master`.
