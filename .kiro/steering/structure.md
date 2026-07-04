# Structure — sosw repository

```
sosw/                     # the package
├── __init__.py           # lazy façade (PEP 562) — public names resolve on demand
├── app.py                # CORE: Processor, LambdaGlobals, get_lambda_handler
├── lambda_api.py         # CORE: LambdaApi(Processor) for API Gateway Lambdas
├── durable.py            # CORE (optional dep): durable execution handler factory + helpers
├── _deprecation.py       # internal deprecation warning helper
├── components/           # AWS middleware + helpers (kept, 100% covered)
│   ├── benchmark.py config.py decorators.py dynamo_db.py exceptions.py
│   ├── helpers.py siblings.py sigv4.py sns.py
│   └── test/unit/        # component unit tests (+ legacy integration/ — not in CI)
├── managers/             # DEPRECATED orchestration managers (task, ecology, meta_handler)
├── orchestrator.py scavenger.py scheduler.py worker.py worker_assistant.py
│                         # DEPRECATED orchestration entities (removed in 4.0)
├── labourer.py essential.py   # DEPRECATED
└── test/
    ├── suite_unit.py     # THE unit suite — every unit test file must be imported here
    ├── unit/             # core unit tests
    ├── integration/      # requires real AWS, not in CI
    └── variables.py, helpers_test*.py  # shared fixtures/mocks

docs/                     # Sphinx documentation (docs.sosw.app), built with -W in CI
examples/                 # runnable examples (SAM, essentials, workers, yaml)
│   └── layers/sosw/      # Lambda layer: build.sh + deploy.sh + README (SSM: lambda-layer-sosw-latest)
cookiecutter/             # cookiecutter template for a new sosw-based Lambda (Processor / LambdaApi)
.github/workflows/        # Tests / Docs / TestPyPI RC / PyPI release
.kiro/specs/              # feature specs (requirements/design/tasks) — committed
.kiro/steering/           # this steering set
AGENTS.md                 # instructions for AI coding agents working on/with sosw
```

## Rules of the road

- New unit test file → import it in `sosw/test/suite_unit.py` or CI never sees it.
- Deprecated modules: bugfixes only; no new features; do not remove before 4.0.
- Public API changes require a spec under `.kiro/specs/` and a docs update in the same release.
- `examples/` content must actually run against the published package version it ships with.
- `AGENTS.md` and `cookiecutter/` mirror the public API and conventions — update them in the same
  PR that changes signatures or config behavior (the generated project's tests must keep passing).
