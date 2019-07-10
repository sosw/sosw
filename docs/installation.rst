Installation
============

Steps
-----

.. toctree::
   :numbered:

#. Setup AWS Account
#. Provision Required AWS Resources
#. Provision Lambda Functions for Essentials
#. Upload Essentials Configurations
#. Create Scheduled Rules

- Scheduler
- Orchestrator
- Scavenger
- WorkerAssistant
- Worker

Setup AWS Account
-----------------

As an AWS Lambda Serverless implementation deployment should be done in an AWS account. To setup a new account, follow
the `AWS Documentation <https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`_

Provision Required AWS Resources
--------------------------------

There are three tables required to run SOSW

- close_tasks
- retry_tasks
- tasks

These can be setup with the provided example :download:`CloudFormation template </yaml/sosw-shared-dynamodb.yaml>`
easily and includes both a testing set of tables along with a production set.

The following Guide assumes that you are running these comands from some machine using either Key or Role
with permissions to control IAM, Lambda, CloudWatch, DynamoDB (and probably something else will come).

.. warning:: Do not run this in Production environment unless you completely understand what is going on!

The following commands are tested on a fresh EC2 instance running on default Amazon Linux 2 AMI.

.. code-block:: bash

   # Install base system
   sudo yum update -y
   sudo yum install git python3.7 -y

   sudo pip3 install -U pipenv pytest boto3 pip

   sudo mkdir /var/app
   sudo chown ec2-user:ec2-user /var/app

   cd /var/app
   git clone https://github.com/bimpression/sosw.git
   cd sosw

   # Creating AWS CloudFormation stacks with required resources.

   # DynamoDB tables
   aws cloudformation create-stack --stack-name=sosw-dev-dynamodb-tables \
   --template-body=file://docs/yaml/sosw-shared-dynamodb.yaml

   # A bucket for artifacts
   aws cloudformation create-stack --stack-name=sosw-dev-s3-bucket \
   --template-body=file://docs/yaml/sosw-s3-bucket.yaml


| Now take a break and wait for these resourced to be created.
| You may enjoy the changes in CloudFormation GUI or make some coffee.


Provision Lambda Functions for Essentials
-----------------------------------------

In this tutorial we use AWS SAM for provisioning Lambdas.

Unfortunately the tutorial is not yet ready, but the result should have four Lambdas all importing ``sosw`` from PyPI.
Example code for Orchestrator is in :download:`/sam/orchestrator/app.py`.
The only dependency in requirements.txt for SAM is ``sosw`` package.


Upload Essentials Configurations
--------------------------------

sosw-managed Lambdas will automatically try to read their configuration from the DynamoDB table ``config``.
Each Lambda looks for the document with hash_key ``config_name = 'LAMBDA_NAME_config'``.
e.g. ``'sosw_orchestrator_config'``

The ``config_value`` should contain JSON-ified dictionary that will be recursively merged to the ``DEFAULT_CONFIG``
of each Lambda.

Please take your time to read more about :ref:`Config` and find the examples in :ref:`Orchestrator`,
:ref:`Scavenger`, :ref:`Scheduler`., etc.


Create Scheduled Rules
----------------------

The usual implementation expects the ``Orchestrator`` and ``Scavenger`` to run every minute, while ``Scheduler``
and ``WorkerAssistant`` are executed per request. ``Scheduler`` may have any number of cronned Business Tasks with any
desired periodicity of course.

