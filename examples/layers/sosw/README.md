# sosw Lambda layer

Build and publish an AWS Lambda layer with the `sosw` framework, so your functions ship only
their own code and import `sosw` (and friends) from the layer.

**Contents of the layer** (python/ directory layout):

- `sosw` (pinned release; `boto3`/`botocore` come along as its dependency — AWS recommends
  pinning your own copy instead of relying on the runtime-bundled one),
- default-on extras: [`aws-lambda-powertools`](https://docs.powertools.aws.dev/lambda/python/latest/)
  (sosw uses its `Logger` automatically when present) and `aws-xray-sdk`. Disable with `--no-extras`.

**Compatible runtimes:** `python3.10`, `python3.11`, `python3.12`, `python3.13`, `python3.14`.

## Build

```bash
./build.sh                              # sosw==3.0.0 + powertools + xray -> ./sosw-layer.zip
./build.sh --no-extras                  # only sosw + its dependencies
./build.sh --sosw-version 3.0.1         # pin another release
./build.sh --use-local                  # pre-release: install sosw from this repo checkout
./build.sh --output /tmp/sosw-layer.zip
```

Build with the same Python minor version as your Lambda runtime when possible
(override the interpreter with `PYTHON=python3.13 ./build.sh`). All bundled packages are
pure Python, so the zip is architecture- and platform-independent.

## Deploy

```bash
./deploy.sh                             # publishes ./sosw-layer.zip + updates SSM pointer
./deploy.sh --zip /tmp/sosw-layer.zip --region us-east-1 --profile my-profile
./deploy.sh --dry-run                   # print the AWS commands without executing
```

`deploy.sh` publishes a new layer version and writes its ARN to the SSM parameter
**`lambda-layer-sosw-latest`**. It is safe to re-run: when the latest published layer version
already has the same content hash (`CodeSha256`) as the local zip, publishing is skipped and only
the SSM pointer is reconciled. Region and credentials come from the standard AWS environment
(`AWS_PROFILE`, `AWS_REGION`, ...) unless overridden with `--region` / `--profile`.

Required permissions: `lambda:PublishLayerVersion`, `lambda:ListLayerVersions`,
`lambda:GetLayerVersion`, `ssm:GetParameter`, `ssm:PutParameter`.

## Consume the layer in a SAM template

Resolve the ARN from SSM at deploy time — templates never hardcode layer versions:

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
      Runtime: python3.13
      Layers:
        - !Ref LambdaLayerSoswLatestArn
```

Already-deployed functions keep the layer version they were deployed with; they pick up the new
ARN on their next `sam deploy`. The cookiecutter template in
[`cookiecutter/`](../../../cookiecutter/) scaffolds a project wired to this parameter.
