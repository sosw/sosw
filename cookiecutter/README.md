# Cookiecutter template: AWS Lambda on sosw

Scaffolds a complete, deployable AWS Lambda project built on the
[`sosw`](https://github.com/sosw/sosw) framework: application code, pure unit tests,
an AWS SAM template consuming the `sosw` Lambda layer, and deploy instructions.

## Usage

```bash
pip install cookiecutter        # or: pipx install cookiecutter

# Straight from GitHub:
cookiecutter https://github.com/sosw/sosw --directory cookiecutter

# Or from a local clone:
cookiecutter path/to/sosw/cookiecutter
```

Answer the prompts (defaults in parentheses):

| Option | Meaning |
|---|---|
| `project_slug` | Python-friendly project directory name (`my_sosw_lambda`) |
| `function_name` | Lambda `FunctionName` and CloudFormation stack name, kebab-case (derived from the slug) |
| `description` | One-line description used in the template and docstrings |
| `python_version` | Lambda runtime version (`3.13`; also 3.10 / 3.11 / 3.12 / 3.14) |
| `use_lambda_api` | `yes` scaffolds an HTTP API Lambda on `sosw.lambda_api.LambdaApi` (declarative routes, auth, JSON envelopes); `no` (default) scaffolds a plain event Processor |
| `aws_region` | Region written to `samconfig.toml` |

## What you get

```
my_sosw_lambda/
├── src/
│   └── app.py           # Processor (or LambdaApi) subclass + lambda_handler wiring
├── test/
│   └── test_unit.py     # pure unit tests: mocked boto3/config, warm-container regression test
├── template.yaml        # AWS SAM template consuming the sosw layer from SSM
├── samconfig.toml       # minimal SAM deploy configuration
├── requirements.txt
└── README.md            # build / test / deploy instructions
```

## After generation

```bash
cd my_sosw_lambda
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest test/ -q          # tests pass out of the box, no AWS needed
sam build && sam deploy --parameter-overrides Stage=dev    # first deploy to an account
```

The SAM template expects the `sosw` Lambda layer ARN in the SSM parameter
`lambda-layer-sosw-latest` — publish it once per account/region with the scripts in
[`examples/layers/sosw/`](../examples/layers/sosw/).
