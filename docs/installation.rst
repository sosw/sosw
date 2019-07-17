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


Setup AWS Account
-----------------

As an AWS Lambda Serverless implementation deployment should be done in an AWS account. To setup a new account, follow
the `AWS Documentation <https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`_

Provision Required AWS Resources
--------------------------------

There are three tables required to run SOSW. Nobody should touch these tables except sosw essentials.

- close_tasks
- retry_tasks
- tasks

Plus there is another table that you may use for your sosw-managed lambdas as well - `config`. Unfortunately the
AWS SSM Parameter Store has pretty low IO limits and having hundreds of parallel executions starts throttling.
`sosw` introduces similar mechanism for accessing configurations and parameters using DynamoDB as a storage.
All functions inheriting from core `sosw.Processor` will fetch their config automatically.

These can be setup with the provided example :download:`CloudFormation template </yaml/sosw-shared-dynamodb.yaml>`
easily and includes both a testing set of tables along with a production set.

The following Guide assumes that you are running these comands from some machine using either Key or Role
with permissions to control IAM, Lambda, CloudWatch, DynamoDB (and probably something else will come).

.. warning:: Do not run this in Production AWS Account unless you completely understand what is going on!

The following commands are tested on a fresh EC2 instance running on default Amazon Linux 2 AMI.

.. code-block:: bash

   # Install required system packages for SAM, AWS CLI and Python.
   sudo yum update -y
   sudo yum install zlib-devel build-essential python3.7 python3-devel git docker -y

   # Update pip and ensure you have required Python packages locally for the user.
   # You might not need all of them at first, but if you would like to test `sosw` or play with it run tests
   sudo pip3 install -U pip pipenv

   sudo mkdir /var/app
   sudo chown ec2-user:ec2-user /var/app

   cd /var/app
   git clone https://github.com/bimpression/sosw.git
   cd sosw

   # Need to configure your AWS CLI environment.
   # Assuming you are using a new machine we shall just copy config with default region `us-west-2` to $HOME.
   # The credentials you should not keep in the profile. The correct secure way is to use IAM roles
   # if running from the AWS infrastructure. Feel free to change or skip this step if your environment is configured.
   cp -nr .aws ~/


Now you are ready to start creating AWS resources. First let us provide some shared resources that both
`sosw` Essentials and `sosw`-managed Lambdas will use.


.. code-block:: bash

   # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
   ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
      grep AccountId | awk -F "\"" '{print $4}'`

   # Set your bucket name
   BUCKETNAME=sosw-s3-$ACCOUNT

   PREFIX=/var/app/sosw/examples/yaml

   # Create new CloudFormation stacks
   for filename in `ls $PREFIX`; do

      stack=`echo $stack | sed s/.yaml//`

      aws cloudformation create-stack --stack-name=$stack \
         --template-body=file://$PREFIX/$filename

   done

| Now take a break and wait for these resourced to be created.
| You may enjoy the changes in CloudFormation GUI or make some coffee.

If you later make any changes in these files (after the initial deployment), use the following script
and it will update CloudFormation stacks.

.. code-block:: bash

   # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
   ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
      grep AccountId | awk -F "\"" '{print $4}'`

   # Set your bucket name
   BUCKETNAME=sosw-s3-$ACCOUNT

   PREFIX=/var/app/sosw/examples/yaml

   # Package and Deploy CloudFormation stacks
   for filename in `ls $PREFIX`; do

      stack=`echo $stack | sed s/.yaml//`
      aws cloudformation package --template-file $PREFIX/$filename \
         --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

      aws cloudformation deploy --template-file /tmp/deployment-output.yaml --stack-name $stack \
         --capabilities CAPABILITY_NAMED_IAM

   done


Provision Lambda Functions for Essentials
-----------------------------------------

In this tutorial we were first going to use AWS SAM for provisioning Lambdas, but eventually gave it up.
Too many black magic is required and you eventually loose control over the Lambda. The example of deploying Essentials
uses raw bash scripts, AWS CLI and CloudFormation templates. If you want to contribute providing examples
with SAM - welcome. Some sandbox can be found in `examples/sam/` in the repository.

Unfortunately the tutorial is not yet ready, but the result should have four Lambdas all importing ``sosw`` from PyPI.
Example code for Orchestrator is in :download:`/sam/orchestrator/app.py`.
The only dependency in requirements.txt for SAM is ``sosw`` package.


.. warning:: This is still unfinished tutorial. Use wizely.

Non-hipster way just manually building the package and creating CF stack with raw CloudFormation.
Gives you full control over what is happening with your services.

.. code-block:: bash

   # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
   ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
      grep AccountId | awk -F "\"" '{print $4}'`

   # Set your bucket name
   BUCKETNAME=sosw-s3-$ACCOUNT


   for name in `ls /var/app/sosw/examples/essentials`; do
       echo "Deploying $name"

      FUNCTION=$name
      FUNCTIONDASHED=`echo $name | sed s/_/-/g`

      cd /var/app/sosw/examples/essentials/$FUNCTION

      # Install sosw package locally. The only dependency is boto3, but we shall have it in Lambda already.
      # Saving a lot of packages size ignoring this dependency. We don't care which exactly pip to use, install locally.
      pip3 install -r requirements.txt --no-dependencies --target .

      # Make a source package. TODO is skip 'dist-info' and 'test' paths. Probably use `find` for this.
      zip -r /tmp/$FUNCTION.zip *

      # Upload the file to S3, so that AWS Lambda will be able to easily take it from there.
      aws s3 cp /tmp/$FUNCTION.zip s3://$BUCKETNAME/sosw/packages/

      # Create CloudFormation Stack with Function resource and deploy it.
      # aws cloudformation create-stack --stack-name=$FUNCTIONDASHED \
      # --template-body=file://yaml/$FUNCTIONDASHED.yaml

      # Package and Deploy CloudFormation stack for the Function.
      # It will create the Function and a custom IAM role for it with permissions to required DynamoDB tables.
      aws cloudformation package --template-file $FUNCTIONDASHED.yaml \
         --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

      aws cloudformation deploy --template-file /tmp/deployment-output.yaml --stack-name $FUNCTIONDASHED \
         --capabilities CAPABILITY_NAMED_IAM
   done

If you change anything in the code or simply want to redeploy the code use the following simple commands: 


.. code-block:: bash

   # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
   ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
      grep AccountId | awk -F "\"" '{print $4}'`

   # Set your bucket name
   BUCKETNAME=sosw-s3-$ACCOUNT

   for name in `ls /var/app/sosw/examples/essentials`; do
       echo "Deploying $name"

       cd /var/app/sosw/examples/essentials/$name
       zip -r /tmp/$name.zip *
       aws lambda update-function-code --function-name $name --s3-bucket $BUCKETNAME \
         --s3-key sosw/packages/$name.zip --publish$
   done

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

