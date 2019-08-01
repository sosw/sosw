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

- Latest documentation is compiled from branch ``docme``.
  It should be up to date with latest **staging** branch, not the master.
  Make PRs with documentation change directly to ``docme``.


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


.. _the example: https://raw.githubusercontent.com/sosw/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml
.. _the tutorial: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html

Building the docs
------------------

| To build the docs locally, run: ``sphinx-build -ab html ./docs ./sosw-rtd``
| Then you can use the built in python web server: ``python -m http.server`` to view the html version
  directly from localhost in your preferred browser.
