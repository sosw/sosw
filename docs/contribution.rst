.. _Contribution Guidelines:

=======================
Contribution Guidelines
=======================

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

* Fork the repository: https://github.com/bimpression/sosw_

* Register Account in AWS: `SignUp`_

* Create DynamoDB Tables:
  * You can find the Cloudformation template for the databases in `the example`_.
  * If you are not familiar with CloudFormation, we highly recommend at least learning the basics from `the tutorial`_.

* Create Sandbox Lambda with Scheduler

* Play with it.

* Read the :ref:`Documentaion convention`


.. _the example: https://raw.githubusercontent.com/bimpression/sosw/docme/docs/yaml/sosw-shared-dynamodb.yaml
.. _the tutorial: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.Walkthrough.html
.. _SignUp: https://portal.aws.amazon.com/billing/signup#/start


