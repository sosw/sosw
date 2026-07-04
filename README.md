<img alt="sosw - a framework for bootstrapping AWS Lambda functions" width="350" src="https://raw.githubusercontent.com/sosw/sosw/docme/docs/_static/images/logo/sosw_black.png">

# sosw
[![Tests](https://github.com/sosw/sosw/actions/workflows/run-unittests.yml/badge.svg)](https://github.com/sosw/sosw/actions/workflows/run-unittests.yml)
[![Docs](https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml/badge.svg)](https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/sosw?color=blue&label=pypi%20installs)](https://pypi.org/project/sosw/)
[![PyPI - Licence](https://img.shields.io/pypi/l/sosw?color=blue)](https://github.com/sosw/sosw/blob/master/LICENSE)

**sosw** is a Python framework for bootstrapping AWS Lambda functions.

Every Lambda gets the same production-grade skeleton in a dozen lines: the `Processor` base class with layered configuration (code defaults + DynamoDB/SSM overrides), automatic initialization of AWS clients, statistics counters, and a generated handler that caches the Processor across warm invocations. On top of that: `LambdaApi` — a declarative router for functions behind API Gateway, optional [AWS durable execution](https://docs.sosw.app/durable.html) support, and a battle-tested library of components and helpers. The only runtime dependency is `boto3`.

## Quick example

```python
from sosw.app import LambdaGlobals, get_lambda_handler, Processor as SoswProcessor


class Processor(SoswProcessor):

    DEFAULT_CONFIG = {
        'init_clients': ['sts'],    # Automatically initialize `self.sts_client`.
    }

    sts_client = None


    def __call__(self, event, **kwargs):
        super().__call__(event)
        return {'account': self.sts_client.get_caller_identity()['Account']}


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
```

The Processor initializes once per Lambda container; warm invocations reuse it. Configuration can be overridden per function through the DynamoDB `config` table or SSM — no redeployment needed.

## Installation

```bash
pip install sosw                # Python 3.12 - 3.14
pip install sosw[durable]       # + AWS durable execution support
```

## Documentation

[https://docs.sosw.app](https://docs.sosw.app)

- [Quickstart](https://docs.sosw.app/quickstart.html) — first Processor Lambda in minutes
- [Concepts](https://docs.sosw.app/concepts/index.html) — Processor, warm start, configuration
- [LambdaApi](https://docs.sosw.app/lambda_api.html) — HTTP APIs from a single Lambda
- [Durable functions](https://docs.sosw.app/durable.html) — long-running checkpointed workflows
- [Migration guide](https://docs.sosw.app/migration_3_0.html) — upgrading from the 0.7.x line

## Coming from the 0.7.x line?

`sosw` began as the *Serverless Orchestrator of Serverless Workers*. The self-hosted orchestration layer (`Orchestrator`, `Scheduler`, `Scavenger`, `Worker`, and their managers) was **removed in the 3.0 major release**. Teams that use it should pin `pip install 'sosw<3'` (the 0.7.x line keeps working, and its docs are preserved at [docs.sosw.app/previous/0.7.51](https://docs.sosw.app/previous/0.7.51/index.html)) and plan the move to AWS Step Functions, EventBridge Scheduler, or durable functions — guidance in the [migration guide](https://docs.sosw.app/migration_3_0.html).

## Development

### Getting started

Either `pipenv`:

```bash
pipenv sync --dev && pipenv shell
```

or plain `pip` in any virtual environment:

```bash
pip install boto3 pytest pytest-cov -r docs/requirements.txt
```

All package metadata lives in `pyproject.toml` (there is no `setup.py`).

### Running tests

The unit suite is explicitly registered in `sosw/test/suite_unit.py` — new test files must be added there. It runs fully mocked (no AWS access) and is enforced at 100% line coverage in CI:

```bash
pytest ./sosw/test/suite_unit.py
pytest ./sosw/test/suite_unit.py --cov=sosw --cov-report=term-missing
```

### Building the docs

```bash
pip install -r docs/requirements.txt
python -m sphinx -W -a -b html docs sosw-rtd; (cd sosw-rtd && python -m http.server)
```

### Contribution guidelines

The full [Contribution Guidelines](https://docs.sosw.app/contribution/index.html) with examples are in the documentation.

#### Release cycle

We follow both the [Semantic Versioning](https://semver.org/) pattern and [PEP440](https://www.python.org/dev/peps/pep-0440/) recommendations where they comply:

- Branches for planned staging versions follow the pattern `X_Y_Z` (Major_Minor_Micro), e.g. `3_0_1`.
- Make your pull request against the closest staging branch (the smallest version after the latest release, of either the current or the next Minor).
- Pushes to staging branches automatically publish release candidates to [TestPyPI](https://test.pypi.org/project/sosw/).
- `master` merges are automatically packaged and published to [PyPI](https://pypi.org/project/sosw/).
- Keep your branch up to date with the branch you are making a PR to.

Example: if the latest released version on PyPI is `3.4.0`, the open staging branches are `3_4_1` and `3_5_0`. A bugfix PR targets `3_4_1`; a new feature targets `3_5_0`.

#### Code style

Follow [PEP8](https://www.python.org/dev/peps/pep-0008/), with the following specifications:
- both classes and functions are padded with 2 empty lines
- dictionaries are value-aligned

## Copyright

This document has been placed in the public domain.
    
    sosw - a framework for bootstrapping AWS Lambda functions
    
    The MIT License (MIT)
    Copyright (C) 2026  sosw core contributors <info@sosw.app>:
        Nikolay Grishchenko
        Sophie Fogel
        Gil Halperin
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
