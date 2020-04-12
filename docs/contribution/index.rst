.. _Contribution Guidelines:

=======================
Contribution Guidelines
=======================


..  toctree::
    :caption: See Also

    Documentation Convention <convention>
    Sprinting PyCon US 2019 <pycon-us-2019>

Release cycle
-------------

- Master branch commits are automatically packaged and published to PyPI.

- Branches for staging versions follow the pattern: ``X_X_X``

- Make your pull requests to the staging branch with highest number


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

Building the docs
------------------

To build the docs locally, run: ``sphinx-build -ab html ./docs ./sosw-rtd``

You can also use the built in python web server to view the html version directly from localhost
in your preferred browser.

.. code-block:: bash

 sphinx-build -ab html ./docs ./sosw-rtd; (cd sosw-rtd && python -m http.server)

Pull Requests
------------

Great that you are ready to contribute!

* Make sure your code passes all the tests

* Make sure the documentation builds correctly

* Push the changes to your fork

* Make a PR to the base repository of sosw

Some guidelines of how to do that can be found in `GitHub documentation`_.


.. _the example: https://raw.githubusercontent.com/sosw/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml
.. _the tutorial: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html
.. _GitHub documentation>: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork
