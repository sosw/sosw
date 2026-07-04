.. _Contribution Guidelines:

=======================
Contribution Guidelines
=======================


..  contents::

..  toctree::

    Documentation Convention <convention>
    Sprinting PyCon US 2019 <pycon-us-2019>

Great that you are ready to contribute! Development happens on GitHub:
`sosw/sosw <https://github.com/sosw/sosw>`_.


Release cycle
-------------

- We follow both the `Semantic Versioning`_ pattern and PEP440_ recommendations where they comply.
- Branches for planned staging versions follow the pattern ``X_Y_Z`` (Major_Minor_Micro),
  e.g. ``3_0_1``.
- Make your pull request against the closest staging branch — the one with the smallest version
  after the latest release, of either the current or the next Minor.
- Pushes to staging branches automatically publish release candidates to
  `TestPyPI <https://test.pypi.org/project/sosw/>`_.
- Merges to ``master`` are automatically packaged and published to
  `PyPI <https://pypi.org/project/sosw/>`_ — ``master`` *is* the release.
- Keep your branch up to date with the branch you are making a PR to.

Example: the latest released version on PyPI is ``3.0.0``; the open staging branches are
``3_0_1`` and ``3_1_0``. A bugfix PR goes to ``3_0_1``; a new feature to ``3_1_0``.

.. _`Semantic Versioning`: https://semver.org/
.. _PEP440: https://www.python.org/dev/peps/pep-0440/


Development setup
-----------------

..  code-block:: bash

    git clone https://github.com/YOUR_FORK/sosw.git && cd sosw

    # Either pipenv:
    pipenv sync --dev && pipenv shell

    # ... or plain pip in any virtual environment:
    pip install boto3 pytest pytest-cov -r docs/requirements.txt

The package itself depends only on ``boto3``; everything else is for tests and docs.
All metadata lives in ``pyproject.toml`` (there is no ``setup.py`` since 3.0.0).


Code style
----------

We follow PEP8_ with the repository-specific details below. CI and review enforce them.

- Maximum line width: 120 characters.
- Both classes and functions/methods are padded with **two** empty lines.
- Dictionary values are vertically aligned.
- Single quotes for regular strings and keys; double quotes for logging and exception messages.
- Logging uses ``%`` formatting, never f-strings: ``logger.info("Got %s", thing)``.
- Imports at the top of the module, grouped: full core, full custom, partial core, partial custom;
  alphabetical within each group.
- No ``try/except: pass`` around business logic — fail fast and loud.
- Data fields are ``snake_case``. Every file ends with exactly one trailing newline.
- Docstrings in Sphinx-friendly reST (``:param x:``, ``:rtype:``) — see the
  :ref:`Documentation Convention`.

.. _PEP8: https://www.python.org/dev/peps/pep-0008/


Tests
-----

The unit suite is **explicitly registered** in ``sosw/test/suite_unit.py``: every new test file
must be imported there and added to the suite, following the existing pattern — orphan test files
do not run in CI.

..  code-block:: bash

    pytest sosw/test/suite_unit.py

Rules of the suite:

- Pure unit tests only: mock ``boto3`` (``patch('boto3.client')``,
  ``patch.object(Processor, 'get_config')``) — no network, no real AWS. The whole suite runs in
  a few seconds.
- Set ``os.environ['STAGE'] = 'test'`` and ``os.environ['autotest'] = 'True'`` *before* importing
  ``sosw`` modules in a test file.
- Test files live in ``sosw/test/unit/``, ``sosw/components/test/unit/`` and
  ``sosw/managers/test/unit/``.

**Coverage bar: 100%.** CI runs the suite with ``--cov=sosw`` and fails below the enforced
threshold (``.coveragerc`` excludes the test directories themselves):

..  code-block:: bash

    pytest sosw/test/suite_unit.py --cov=sosw --cov-report=term-missing

..  image:: /_static/images/coverage.svg
    :alt: Test Coverage


Continuous integration
----------------------

GitHub Workflows run on every pull request:

- **Tests** (``run-unittests.yml``) — the unit suite on a Python 3.12 / 3.13 / 3.14 matrix (the
  three latest versions; the package itself supports 3.10+), plus a coverage job enforcing the
  threshold on Python 3.14.
- **Docs** (``docs-builder-action.yaml``) — strict Sphinx build; **any warning fails the build**.
- **TestPyPI** (``publish-to-test-pypi.yml``) — publishes release candidates from ``X_Y_Z``
  staging branches.
- **PyPI** (``publish-to-pypi.yml``) — publishes from ``master``.
- **Publish Docs** (``publish-docs.yml``) — publishes the built docs to https://docs.sosw.app
  automatically on every push to ``master`` (OIDC-assumed AWS role, no static keys); a manual
  dispatch offers a dry-run mode.


Building the docs
-----------------

..  code-block:: bash

    pip install -r docs/requirements.txt
    python -m sphinx -W -a -b html docs sosw-rtd

    # View at http://localhost:8000
    (cd sosw-rtd && python -m http.server)

The ``-W`` flag mirrors CI: warnings are errors. Fix them, do not silence them.


Pull request checklist
----------------------

* Your branch is up to date with the staging branch you target.
* ``pytest sosw/test/suite_unit.py`` passes, new code is covered, new test files are registered
  in ``suite_unit.py``.
* ``python -m sphinx -W -a -b html docs sosw-rtd`` passes if you touched docs or docstrings.
* The code follows the style rules above.
* Open the PR against the appropriate ``X_Y_Z`` staging branch (not ``master``).

Guidelines for creating PRs from forks are in the `GitHub documentation`_.

.. _GitHub documentation: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork
