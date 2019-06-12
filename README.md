# Serverless Orchestrator of Serverless Workers (SOSW)
[![Build Status](https://travis-ci.org/bimpression/sosw.svg?branch=master)](https://travis-ci.org/bimpression/sosw)

**sosw** is a set of tools for orchestrating asynchronous invocations of AWS Lambda Workers.

## Documentation
[Read The Docs: sosw.readthedocs.io](https://sosw.readthedocs.io/en/latest/)

## Dependencies
- Python 3.6, 3.7
- [boto3](https://github.com/boto/boto3) (AWS SDK for Python)

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
1. Fork the repository: https://github.com/bimpression/sosw
2. Register Account in AWS: [SignUp](https://portal.aws.amazon.com/billing/signup#/start)
3. Run `pipenv sync –dev` to setup your virtual environment and download the required dependencies
4. Create DynamoDB Tables: 
    - You can find the CloudFormation template for the databases [in the example](https://raw.githubusercontent.com/bimpression/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml).
    - If you are not familiar with CloudFormation, we highly recommend at least learning the basics from [the tutorial](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html).
5. Create Sandbox Lambda with Scheduler
6. Play with it.
7. Read the Documentation Convention.

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
