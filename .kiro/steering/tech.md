# Tech standards — sosw

## Language & dependencies

- Python 3.10–3.14. Runtime dependency: `boto3` only. Optional extras: `sosw[durable]`.
- Never add a mandatory dependency without a maintainer decision recorded in an issue.
- Packaging via `pyproject.toml` (PEP 621, setuptools backend). Version lives there.

## Code style (PRs are bounced on violations)

- PEP-8 with **two blank lines** between functions/methods/classes; vertically aligned dict values.
- **Single quotes** for regular strings and dict keys; **double quotes** for logging and exception
  messages.
- Logging always `logger.info("... %s", var)` — **never f-strings in log calls**.
- Imports at the top, grouped: full core imports, full custom imports, partial core, partial custom.
- No `try/except: pass` around business logic. Fail fast and loud — Lambdas must raise on errors.
- snake_case for data fields in events/payloads/records.
- reST docstrings (`:param x:`, `:rtype:`) on all public callables; modules start with the MIT license
  header docstring (copy from `sosw/app.py`).
- Every file ends with exactly one newline.

## Testing

- `unittest.TestCase` style; unit tests live in `*/test/unit/` and MUST be registered in
  `sosw/test/suite_unit.py` (explicit suite — pytest discovery is NOT used in CI).
- Unit tests are pure: mock boto3 (`patch('boto3.client')`, `MagicMock` clients,
  `patch.object(Processor, 'get_config')`). No network, no real AWS, no sleeps.
- Integration tests (`*/test/integration/`) require real AWS and are NOT run in CI.
- Do not run `pytest sosw/` over the whole tree — legacy integration files create real DynamoDB tables.
- Coverage: 100% of `sosw/` (test dirs excluded via `.coveragerc`), enforced with
  `--cov-fail-under` in CI. Cover optional-import guards by reloading modules with a patched
  `sys.modules`, not with `# pragma: no cover`.

## CI/CD

- GitHub Workflows only (Travis is gone): Tests matrix 3.10–3.14 + coverage job (100%), Docs build
  (Sphinx `-W`), TestPyPI RC publish on staging branches (`X_Y_Z`), PyPI publish on push to `master`.
- Staging-branch flow: feature branches → PR into the current `X_Y_Z` staging branch → release PR
  `X_Y_Z → master`. Merging to master publishes to PyPI.

## Bootstrap artifacts (ship with the repo, must track the API)

- `AGENTS.md` — coding-agent instructions; encodes the build/test/debug/style conventions.
- `cookiecutter/` — project template scaffolding a sosw-based Lambda (plain Processor or LambdaApi
  variant); generated projects' unit tests must pass against the current package.
- `examples/layers/sosw/` — Lambda layer build/deploy scripts (sosw + boto3, default-on
  powertools/xray extras; SSM pointer `lambda-layer-sosw-latest`).
- A PR changing public API signatures or config conventions updates these in the same PR.
