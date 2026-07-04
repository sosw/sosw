# Structure — sosw repository

```
sosw/                     # the package
├── __init__.py           # lazy façade (PEP 562) — public names resolve on demand
├── app.py                # CORE: Processor, LambdaGlobals, get_lambda_handler
├── lambda_api.py         # CORE: LambdaApi(Processor) for API Gateway Lambdas
├── durable.py            # CORE (optional dep): durable execution handler factory + helpers
├── components/           # AWS middleware + helpers (100% covered)
│   ├── benchmark.py config.py decorators.py dynamo_db.py exceptions.py
│   ├── helpers.py siblings.py sigv4.py sns.py
│   └── test/unit/        # component unit tests (+ legacy integration/ — not in CI)
└── test/
    ├── suite_unit.py     # THE unit suite — every unit test file must be imported here
    ├── unit/             # core unit tests
    ├── test_app__aws.py  # integration test — requires real AWS, not in CI
    └── helpers_test_dynamo_db.py  # shared fixtures/mocks

docs/                     # Sphinx documentation (docs.sosw.app), built with -W in CI
examples/                 # runnable examples (Lambda layer, yaml, config helper)
│   └── layers/sosw/      # Lambda layer: build.sh + deploy.sh + README (SSM: lambda-layer-sosw-latest)
cookiecutter/             # cookiecutter template for a new sosw-based Lambda (Processor / LambdaApi)
.github/workflows/        # Tests / Docs / TestPyPI RC / PyPI release
.kiro/specs/              # feature specs (requirements/design/tasks) — committed
.kiro/steering/           # this steering set
AGENTS.md                 # instructions for AI coding agents working on/with sosw
```

## Rules of the road

- New unit test file → import it in `sosw/test/suite_unit.py` or CI never sees it.
- Public API changes require a spec under `.kiro/specs/` and a docs update in the same release.
- `examples/` content must actually run against the published package version it ships with.
- `AGENTS.md` and `cookiecutter/` mirror the public API and conventions — update them in the same
  PR that changes signatures or config behavior (the generated project's tests must keep passing).
