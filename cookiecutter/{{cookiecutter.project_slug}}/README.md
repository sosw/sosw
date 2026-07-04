# {{ cookiecutter.function_name }}

{{ cookiecutter.description }}

Built on the [sosw](https://github.com/sosw/sosw) framework (docs: https://docs.sosw.app).

## Layout

```
{{ cookiecutter.project_slug }}/
├── src/app.py           # Processor + lambda_handler
├── test/test_unit.py    # pure unit tests (no AWS access required)
├── template.yaml        # AWS SAM template
├── samconfig.toml       # SAM deploy configuration
└── requirements.txt     # local development dependencies
```

## Prerequisites

- AWS SAM CLI and AWS credentials for the target account.
- The `sosw` Lambda layer published in the target account/region, with its ARN stored in the
  SSM parameter `lambda-layer-sosw-latest`. Publish it once with the scripts in
  [`examples/layers/sosw/`](https://github.com/sosw/sosw/tree/master/examples/layers/sosw)
  of the sosw repository.
- Optional: a DynamoDB table named `config` (keys: `env`, `config_name`) if you want to manage the
  runtime configuration of the function without redeploying (see Runtime configuration below).

## Test

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest test/ -q
```

The tests are pure unit tests: `boto3` and the config lookup are mocked, so they run without any
AWS access. They include the warm-container regression test — keep it passing when you add state.

## Deploy

```bash
sam build
sam deploy --parameter-overrides Stage=dev    # FIRST deploy to an account: pass Stage explicitly
sam deploy                                    # subsequent deploys reuse the stack's Stage
```
{% if cookiecutter.use_lambda_api == 'yes' %}

## API

Routes are declared in `DEFAULT_CONFIG['routes']` of `src/app.py`:

| Route | Handler |
|---|---|
| `GET /health` | `get_health` |
| `GET /things/{thing_id}` | `get_thing` |

```bash
curl "$(sam list stack-outputs --output json | jq -r '.[] | select(.OutputKey=="ApiUrl") | .OutputValue')health"
```

Authorization: `auth_enabled` is on by default, so requests without Cognito claims get `401`.
Either attach a Cognito JWT authorizer to the `HttpApi` resource in `template.yaml`:

```yaml
  HttpApi:
    Type: AWS::Serverless::HttpApi
    Properties:
      Auth:
        DefaultAuthorizer: CognitoJwtAuthorizer
        Authorizers:
          CognitoJwtAuthorizer:
            IdentitySource: $request.header.Authorization
            JwtConfiguration:
              issuer: !Sub 'https://cognito-idp.${AWS::Region}.amazonaws.com/YOUR_USER_POOL_ID'
              audience:
                - YOUR_APP_CLIENT_ID
```

or, for a private/dev API without Cognito, set `'auth_enabled': False` in the config.
{% endif %}

## Runtime configuration

`sosw` builds the effective config of the Processor in layers, each recursively updating the
previous one:

1. `DEFAULT_CONFIG` in `src/app.py`;
2. the `{{ cookiecutter.function_name }}_config` record of the DynamoDB `config` table — change
   the behavior of the deployed function without redeploying;
3. `custom_config` passed to the constructor (used by the unit tests).

No `config` table (or no permissions)? The lookup silently yields `{}`. If you do not use DynamoDB
configs at all, set `'disable_ddb_config': True` in `DEFAULT_CONFIG` and drop the `ReadSoswConfig`
policy statement from `template.yaml`.

Debugging tip: pass `"logging_level": "DEBUG"` in the Lambda event to raise the log verbosity of a
single invocation.
