<img alt="sosw - Serverless Orchestrator of Serverless Workers" width="350" src="https://raw.githubusercontent.com/sosw/sosw/docme/docs/_static/images/logo/sosw_black.png">

# Serverless Orchestrator of Serverless Workers
[![Build Status](https://travis-ci.org/sosw/sosw.svg?branch=master)](https://travis-ci.org/sosw/sosw)
[![Documentation Status](https://readthedocs.org/projects/sosw/badge/?version=latest)](https://docs.sosw.app/en/latest/?badge=latest)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/sosw?color=blue&label=pypi%20installs)](https://pypi.org/project/sosw/)
[![PyPI - Licence](https://img.shields.io/pypi/l/sosw?color=blue)](https://github.com/sosw/sosw/blob/master/LICENSE)

**sosw** is a set of serverless tools for orchestrating asynchronous invocations of AWS Lambda Functions (Workers).

---
 Please pronounce **sosw** correctly: _/ˈsɔː səʊ/_

---

## Documentation
[https://docs.sosw.app](https://docs.sosw.app/en/latest/)

## Essential Workflows
![Essential sosw Workflow Schema](https://raw.githubusercontent.com/sosw/sosw/docme/docs/_static/images/simple-sosw.png)

## Dependencies
- Python 3.6, 3.7
- [boto3](https://github.com/boto/boto3) (AWS SDK for Python)

## Installation
See the [Installation Guidelines](https://docs.sosw.app/en/latest/installation.html) in the Documentation.

## Development
### Getting Started

Assuming you have Python 3.6 and `pipenv` installed. Create a new virtual environment: 

```bash
$ pipenv shell
```

Now install the required dependencies for development:

```bash
$ pipenv sync --dev
```

### Running Tests

Running unit tests:
```bash
$ pytest ./sosw/test/suite_3_6_unit.py
```

### Contribution Guidelines

#### Release cycle
- We follow both [Semantic Versioning](https://semver.org/) pattern
  and [PEP440](https://www.python.org/dev/peps/pep-0440/) recommendations where comply
- Master branch commits (merges) are automatically packaged and published to PyPI.
- Branches for planned staging versions follow the pattern: `X_Y_Z` (Major.Minor.Micro)
- Make your pull requests to the latest staging branch (with highest number)
- Latest documentation is compiled from branch `docme`.
  It should be up to date with latest **staging** branch, not the master.
  Make PRs with documentation change directly to `docme`.

#### Code formatting
Follow [PEP8](https://www.python.org/dev/peps/pep-0008/), but:
- both classes and functions are padded with 2 empty lines
- dictionaries are value-alligned

#### Initialization
1. Fork the repository: https://github.com/sosw/sosw
2. Register Account in AWS: [SignUp](https://portal.aws.amazon.com/billing/signup#/start)
3. Run `pipenv sync –dev` to setup your virtual environment and download the required dependencies
4. Create DynamoDB Tables: 
    - You can find the CloudFormation template for the databases [in the example](https://raw.githubusercontent.com/sosw/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml).
    - If you are not familiar with CloudFormation, we highly recommend at least learning the basics from [the tutorial](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html).
5. Create Sandbox Lambda with Scheduler
6. Play with it.
7. Read the Documentation Convention.

#### More
See more guidelines for contribution [in the docs](https://docs.sosw.app/en/latest/contribution/index.html).

### Building the docs
Sphinx is used for building documentation. To build HTML documentation locally, use:

```bash
$ sphinx-build -ab html ./docs ./sosw-rtd
```

You can then use the built in Python web server to view the html version directly from `localhost` in your preferred browser.

```bash
$ cd sosw-rtd
$ python -m http.server
```

## Copyright

This document has been placed in the public domain.
    
    sosw - Serverless Orchestrator of Serverless Workers
    
    The MIT License (MIT)
    Copyright (C) 2019  sosw core contributors <info@sosw.app>:
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
