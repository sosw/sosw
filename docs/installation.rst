Installation
============

Steps
-----

1. Setup AWS Account
2. Provision AWS DynamoDB Tables
3. Provision Lambda Functions for
  - Scheduler
  - Orchestrator
  - Scavenger
  - WorkerAssistant
  - Worker

Setup AWS Account
-----------------

As an AWS Lambda Serverless implementation deployment should be done in an AWS account. To setup a new account, follow the `Link AWS Documentation <https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`

Provision AWS DynamoDB Tables
------------------------------

There are three tables required to run SOSW

- close_tasks
- retry_tasks
- tasks

These can be setup with the provided example CloudFormation template :doc:`/yaml/sosw-shared-dynamo.yaml` easily and includes both a testing set of tables along with a production set.
