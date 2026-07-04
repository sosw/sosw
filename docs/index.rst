.. title:: Home

=============================================
Serverless Orchestrator of Serverless Workers
=============================================

..  image:: https://github.com/sosw/sosw/actions/workflows/run-unittests.yml/badge.svg
    :alt: Tests
    :target: https://github.com/sosw/sosw/actions/workflows/run-unittests.yml
..  image:: https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml/badge.svg
    :alt: Docs
    :target: https://github.com/sosw/sosw/actions/workflows/docs-builder-action.yaml
..  image:: _static/images/coverage.svg
    :alt: Test Coverage
    :target: https://docs.sosw.app/?badge=latest
..  image:: https://img.shields.io/pypi/dm/sosw?color=blue&label=pypi%20installs
    :alt: PyPI - Installs / month
    :target: https://pypi.org/project/sosw/
..  image:: https://img.shields.io/pypi/l/sosw?color=blue
    :alt: PyPI - License
    :target: https://github.com/sosw/sosw/blob/master/LICENSE

**`sosw`**:
 - Framework to simplify the design of AWS Lambda functions in Python
 - Set of tools for orchestrating **asynchronous** invocations of AWS Lambda functions.

Essential components of **`sosw`** orchestration are implemented as AWS Lambda functions themselves.


.. note::
   Please pronounce `sosw` correctly: */ˈsɔːsəʊ/*

..	toctree::
	:titlesonly:
	:caption: Contents:
	:maxdepth: 2

	quickstart
	orchestration
	installation
	essentials/index
	components/index
	managers/index
	tools/index
	tutorials/index

	contribution/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
