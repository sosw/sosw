# Product — sosw

`sosw` (PyPI: `sosw`) is a **Python framework for bootstrapping AWS Lambda functions**.

It gives every Lambda a common backbone:

- `sosw.app.Processor` — a base class with layered configuration (defaults → DynamoDB/SSM config →
  custom config), statistics collection, lazy AWS client registration, and container warm-start reuse
  via `LambdaGlobals` / `get_lambda_handler`.
- `sosw.lambda_api.LambdaApi` — a Processor for HTTP APIs behind API Gateway: declarative routing,
  Cognito claims handling, CORS, JSON envelopes, typed API errors.
- `sosw.durable` — optional integration with AWS Lambda durable execution (`pip install sosw[durable]`).
- `sosw.components` — thin, well-tested middleware for AWS services (DynamoDB, SNS, Secrets/Config,
  SigV4 signing, siblings) plus battle-tested helpers.

## History and positioning

Until 2.x the package was the "Serverless Orchestrator of Serverless Workers" — a self-hosted
orchestration suite (Orchestrator, Scheduler, Scavenger, Worker…). Since **3.0.0** those orchestration
entities are **deprecated** (functional through 3.x, removed in 4.0): AWS-native services
(Step Functions, EventBridge, durable execution) now cover that ground. The framework core —
Processor, components, helpers — is the product.

## Users

- Engineering teams standardizing many small Lambdas on one config/stats/testing pattern.
- AI coding agents scaffolding new Lambdas — entry points: `AGENTS.md` (agent instructions),
  `cookiecutter/` (project template), `examples/layers/sosw/` (Lambda layer build/deploy scripts).

## Quality bars

- 100% unit test line coverage, enforced in CI. Unit suite is network-free and fast (seconds).
- Zero mandatory dependencies beyond `boto3`.
- Python 3.10–3.14 supported; CI-tested on the three latest (3.12–3.14).
- Docs at https://docs.sosw.app must build warning-free.
