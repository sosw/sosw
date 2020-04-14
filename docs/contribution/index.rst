.. _Contribution Guidelines:

=======================
Contribution Guidelines
=======================


..  contents::

..  toctree::

    Documentation Convention <convention>
    Sprinting PyCon US 2019 <pycon-us-2019>

Release cycle
-------------

- Master branch commits are automatically packaged and published to PyPI.

- We follow both `Semantic Versioning`_ pattern and PEP440_ recommendations where comply

- Branches for planned staging versions follow the pattern: ``X_Y_Z`` (Major.Minor.Micro)

- Make your pull requests go to the closest staging branch (with smallest after release number of either current or next Minor)

- Make sure your branch is up to date with the branch you are making a PR to

Example:

  - Latest released version in PyPI ``0.7.31``
  - Closest staging Minor branch in sosw/sosw ``0_7_33``
  - Latest Minor staging branch in sosw/sosw ``0_7_35``
  - Closest Next Minor branch in sosw/sosw ``0_9_1``

Your PR should be to either ``0_7_33`` or ``0_9_1`` depending on the importance of changes.

.. _`Semantic Versioning`: https://semver.org/
.. _PEP440: https://www.python.org/dev/peps/pep-0440/

Code formatting
---------------

Follow the PEP8_ but both classes and functions are padded with 2 empty lines.

.. _PEP8: https://www.python.org/dev/peps/pep-0008/

Initialization
--------------

* Fork the repository: `<https://github.com/sosw/sosw>`_

* Register Account in AWS: `SignUp <https://portal.aws.amazon.com/billing/signup#/start>`_

* Run ``pipenv sync --dev`` to setup your virtual environment and download the required dependencies

* If you are not familiar with CloudFormation, we highly recommend at least learning the basics from `the tutorial`_.

* Follow the :ref:`Installation Guidelines` to setup your environment.

* Create some Sandbox Lambda.

* Play with it.

* Read the :ref:`Documentation convention`

.. _the tutorial: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html

Building the docs
------------------

To build the docs locally, run: ``sphinx-build -ab html ./docs ./sosw-rtd``

You can also use the built in python web server to view the html version directly from localhost
in your preferred browser.

.. code-block:: bash

 sphinx-build -ab html ./docs ./sosw-rtd; (cd sosw-rtd && python -m http.server)

Pull Requests Checklist
-----------------------

Great that you are ready to contribute!

* Make sure your fork is up to date with upstream

..  code-block:: bash

    # Clean everything
    git reset --hard
    git clean -fdx
    git checkout master

    # Fetch possible changes to YOUR master
    git pull origin master

    # Check if remote upstream is configured
    git remote -v

    # If missing upstream
    git remote add upstream https://github.com/sosw/sosw

    # Update your fork remote with the upstream changes
    git pull upstream master
    git push origin master

* Make sure your code passes all the tests

..  code-block:: bash

    pytest sosw/test/suite_3_6_unit.py

* Make sure the documentation builds correctly

..  code-block:: bash

    sphinx-build -ab html ./docs ./sosw-rtd; (cd sosw-rtd && python -m http.server)

* Push the changes to your fork remote

..  code-block:: bash

    git push origin master

* Make a PR to the upstream repository of sosw

Some guidelines of how to do create PRs from forks can be found in `GitHub documentation`_.

.. _GitHub documentation: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork
