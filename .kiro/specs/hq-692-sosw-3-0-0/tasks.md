# sosw 3.0.0 — Tasks

Each numbered task = one PR into `3_0_0` unless noted. Requirements references in brackets.

- [x] 1. CI & repo hygiene (`hq-692-ci-workflows`) [R8, R10-part]
  - [x] 1.1 Commit spec (`.kiro/specs/hq-692-sosw-3-0-0/`) and initial `.kiro/steering/` docs
  - [x] 1.2 `run-unittests.yml`: 3.10–3.14 matrix + coverage job (`--cov-fail-under`, staged threshold)
  - [x] 1.3 `docs-builder-action.yaml`: pip-based, `sphinx-build -W`
  - [x] 1.4 `publish-to-test-pypi.yml`: staging-branch pattern `X_Y_Z`, version patch step
  - [x] 1.5 README badges → GitHub Actions; remove Travis; drop stray `.aws/config`; gitignore `.claude/`
- [x] 2. Deprecations (`hq-692-deprecations`) [R2]
  - [x] 2.1 Deprecation helper + warnings in all 10 deprecated classes
  - [x] 2.2 `sosw/__init__.py` lazy façade (PEP 562), `siblings.py` import fix
  - [x] 2.3 Unit tests: warning emitted once per class, façade lazy, components silent
- [x] 3. lambda_api (`hq-692-lambda-api`) [R4]
  - [x] 3.1 `ApiError` hierarchy in `components/exceptions.py`
  - [x] 3.2 `sosw/lambda_api.py` LambdaApi with router/auth/CORS/encoder/envelope
  - [x] 3.3 Full unit test matrix (v1+v2 events, auth paths, errors, CORS, encoder)
- [x] 4. Warm start + durable (`hq-692-durable`) [R3, R5]
  - [x] 4.1 Extract shared `_make_lambda_handler`; fix `test` precedence; single `reset_stats`; `disable_ddb_config`
  - [x] 4.2 `sosw/durable.py` + helpers, import guards
  - [x] 4.3 Unit tests incl. fake-SDK injection and no-SDK-leak assertions
- [x] 5. Packaging + helpers tidy (`hq-692-packaging`) [R1, R6]
  - [x] 5.1 `pyproject.toml` full metadata, version 3.0.0, extras `[durable]`, 3.14 classifier
  - [x] 5.2 Pipfile/lock refresh or removal (kill the 15 Dependabot alerts)
  - [x] 5.3 `case_insensitive` for `recursive_matches_extract` (PR #379, credited); scheduler raw-string fix
- [x] 6. Coverage 100% (`hq-692-coverage`) [R7]
  - [x] 6.1 Register orphan tests; retire duplicate legacy test_config
  - [x] 6.2 Per-module test authoring to 100% (incl. decorators.py from 0%)
  - [x] 6.3 Powertools import-guard branch coverage via module reload technique
  - [x] 6.4 Raise CI gate to `--cov-fail-under=100`
- [x] 7. Docs rebuild (`hq-692-docs`) [R9]
  - [x] 7.1 Theme + structure + quickstart + concepts + component guides
  - [x] 7.2 lambda_api + durable guides, tutorials refresh, migration_3_0 page
  - [x] 7.3 Deprecated section banner-marked; all examples on 3.0 API
- [x] 8. Agent docs & bootstrap tooling (`hq-692-agent-docs`) [R10, R11]
  - [x] 8.1 `AGENTS.md` (closes #388) + steering refinement
  - [x] 8.2 Cookiecutter template + docs
  - [x] 8.3 Layer example with autobuild/autodeploy scripts (powertools/xray default-on)
- [x] 9. GitHub triage (no PR) [R12]
  - [x] 9.1 Triage comments on all open issues/PRs as bender-sosw; close obsolete
  - [x] 9.2 File new improvement issues
- [x] 10. Release PR `3_0_0 → master` — open, changelog, DO NOT merge (human gate)

## Delivery map

All PRs merged into `3_0_0`, except the task 10 release PR which is open pending the human merge gate.

| Task | Delivered by |
|------|--------------|
| 1    | PR #390 |
| 2    | PR #392 |
| 3    | PR #393 |
| 4    | PR #394 |
| 5    | PR #391 |
| 6    | PR #404 |
| 7    | PR #403 |
| 8    | PR #402 |
| 9    | Triage comments on all open issues/PRs + new issues #395–#401, #405–#406 (no PR) |
| 10   | PR #407 (`3_0_0 → master`, open — human merge gate) |

Post-review polish (release-PR review): CI now runs on the three latest Pythons (3.12–3.14 matrix)
with 3.14 as the default runtime for all CI/CD jobs; the package itself still supports 3.10+.
