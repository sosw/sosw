Installation
============

Steps
-----

#. Setup AWS Account
#. Provision AWS DynamoDB Tables
#. Provision Lambda Functions for

- Scheduler
- Orchestrator
- Scavenger
- WorkerAssistant
- Worker

Setup AWS Account
-----------------

As an AWS Lambda Serverless implementation deployment should be done in an AWS account. To setup a new account, follow the `AWS Documentation <https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`_

Provision AWS DynamoDB Tables
------------------------------

There are three tables required to run SOSW

- close_tasks
- retry_tasks
- tasks

These can be setup with the provided example :download:`CloudFormation template </yaml/sosw-shared-dynamodb.yaml>` easily and includes both a testing set of tables along with a production set.

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
   aws cloudformation create-stack --stack-name=sosw-development-dynamodb-tables --template-body=file://docs/yaml/sosw-shared-dynamodb.yaml

   # A bucket for artifacts
   aws cloudformation create-stack --stack-name=autotest-bucket --template-body=file://docs/yaml/autotest-bucket.yaml


   # Sosw Essentials (AWS Lambdas)
   # Each Lambda has it's own role.

   aws cloudformation create-stack --stack-name=sosw-development-orchestrator --template-body=file://docs/yaml/sosw-orchestrator.yaml --capabilities CAPABILITY_NAMED_IAM


