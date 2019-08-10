.. _Installation Guidelines:


============
Installation
============


Steps
-----
#. `Setup AWS Account`_
#. `Provision Required AWS Resources`_
#. `Provision Lambda Functions for Essentials`_
#. `Upload Essentials Configurations`_
#. `Create Scheduled Rules`_


Setup AWS Account
-----------------
``sosw`` implementation currently supports only AWS infrastructure. If you are running
production operations on AWS, we highly recommend setting up a standalone account for your
first experiments with ``sosw``. `AWS Organisations <https://aws.amazon.com/organizations/>`_
now provide an easy way to set sub-accounts from the primary one.

To setup a completely isolated new account, follow the
`AWS Documentation <https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`_

We shall require several services, but they are all supposed to fit in the
`AWS Free Tier. <https://aws.amazon.com/free/>`_ As long as the resources are created using
CloudFormation, once you delete the stacks - the related resources will also be deleted
automatically to avoid unnecessary charges.

See :ref:`Cleanup` instructions in the :ref:`Tutorials` section.


Provision Required AWS Resources
--------------------------------
This document shall guide you through the setup process for ``sosw`` Essentials and different
resources required for them. All the resources are created using Infrastructure as Code
concept and can be easily cleaned up if no longer required.

..  warning::

    The following Guide assumes that you are running these comands from some EC2 machine using
    either Key or Role with permissions to control IAM, CloudFormation, Lambda, CloudWatch,
    DynamoDB, S3 (and probably something else).

If you are running this in the test account - feel free to grant the IAM role of your EC2
instance the policy ``arn:aws:iam::aws:policy/AdministratorAccess``, but never do this in
Production.

If you plan to run tutorials after this, we recommend setting this up in ``us-west-2``
(Oregon) Region. Some scripts in the tutorials guidelines may have the region hardcoded.

Now we assume that you have created a fresh Amazon Linux 2 machine with some IAM Role having
permissions listed above. You may follow `this tutorial
<https://docs.aws.amazon.com/efs/latest/ug/gs-step-one-create-ec2-resources.html>`_
if feeling uncertain, just create a new IAM Role on Step 3 of the instance setup Wizard.

..  warning::

    Do not run this in Production AWS Account unless you completely understand
    what is going on!

The following commands are tested on a fresh EC2 instance of type ``t2.micro`` running
on default Amazon Linux 2 AMI 64-bit.

..  code-block:: bash

    # Install required system packages for SAM, AWS CLI and Python.
    sudo yum update -y
    sudo yum install zlib-devel build-essential python3.7 python3-devel git docker -y

    # Update pip and ensure you have required Python packages locally for the user.
    # You might not need all of them at first, but if you would like to test `sosw`
    # or play with it run tests
    sudo pip3 install -U pip pipenv boto3

    sudo mkdir /var/app
    sudo chown ec2-user:ec2-user /var/app

    cd /var/app
    git clone https://github.com/sosw/sosw.git
    cd sosw

    # Need to configure your AWS CLI environment.
    # Assuming you are using a new machine we shall just copy config with default region
    # `us-west-2` to $HOME. The credentials you should not keep in the profile.
    # The correct secure way is to use IAM roles if running from the AWS infrastructure.
    # Feel free to change or skip this step if your environment is configured.
    cp -nr .aws ~/


Now you are ready to start creating AWS resources. First let us provide some shared resources
that both ``sosw`` Essentials and ``sosw``-managed Lambdas will use.

..  code-block:: bash

    # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
        grep AccountId | awk -F "\"" '{print $4}'`

    # Set your bucket name
    BUCKETNAME=sosw-s3-$ACCOUNT

    PREFIX=/var/app/sosw/examples/yaml/initial

    # Create new CloudFormation stacks
    for filename in `ls $PREFIX`; do
        stack=`echo $filename | sed s/.yaml//`

        aws cloudformation create-stack --stack-name=$stack \
            --template-body=file://$PREFIX/$filename
    done

..  note::

    Now take a break and wait for these resourced to be created. You may observe the changes
    in the CloudFormation web-console (Services -> CloudFormation).

..  warning:: DO NOT continue until all stacks reach the CREATE_COMPLETE status.

If you later make any changes in these files (after the initial deployment), use the
following script and it will update CloudFormation stacks. No harm to run it extra time.
CloudFormation is smart enough not to take any action if there are no changes in templates.

..  hidden-code-block:: bash
    :label: Show script <br>

    # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
        grep AccountId | awk -F "\"" '{print $4}'`

    # Set your bucket name
    BUCKETNAME=sosw-s3-$ACCOUNT

    PREFIX=/var/app/sosw/examples/yaml/initial

    # Package and Deploy CloudFormation stacks
    for filename in `ls $PREFIX`; do

        stack=`echo $filename | sed s/.yaml//`
        aws cloudformation package --template-file $PREFIX/$filename \
            --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

        aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
            --stack-name $stack --capabilities CAPABILITY_NAMED_IAM
    done


Provision Lambda Functions for Essentials
-----------------------------------------
In this tutorial we were first going to use AWS SAM for provisioning Lambdas,
but eventually gave it up. Too many black magic is required and you eventually loose
control over the Lambda. The example of deploying Essentials uses raw bash/python scripts,
AWS CLI and CloudFormation templates. If you want to contribute providing examples
with SAM - welcome. Some sandbox can be found in `examples/sam/` in the repository.

..  code-block:: bash

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

        # Install sosw package locally.
        pip3 install -r requirements.txt --no-dependencies --target .

        # Make a source package.
        zip -qr /tmp/$FUNCTION.zip *

        # Upload the file to S3, so that AWS Lambda will be able to easily take it from there.
        aws s3 cp /tmp/$FUNCTION.zip s3://$BUCKETNAME/sosw/packages/

        # Package and Deploy CloudFormation stack for the Function.
        # It will create the Function and a custom IAM role for it with permissions
        # to access required DynamoDB tables.
        aws cloudformation package --template-file $FUNCTIONDASHED.yaml \
            --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

        aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
            --stack-name $FUNCTIONDASHED --capabilities CAPABILITY_NAMED_IAM
    done


If you change anything in the code or simply want to redeploy the code use the following:

..  hidden-code-block:: bash
    :label: Show script <br>

    # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
      grep AccountId | awk -F "\"" '{print $4}'`

    # Set your bucket name
    BUCKETNAME=sosw-s3-$ACCOUNT

    for name in `ls /var/app/sosw/examples/essentials`; do
      echo "Deploying $name"

      FUNCTIONDASHED=`echo $name | sed s/_/-/g`

      cd /var/app/sosw/examples/essentials/$name
      zip -qr /tmp/$name.zip *
      aws lambda update-function-code --function-name $name --s3-bucket $BUCKETNAME \
        --s3-key sosw/packages/$name.zip --publish

      # Package and Deploy (if there are changes) CloudFormation stack for the Function.
      aws cloudformation package --template-file $FUNCTIONDASHED.yaml \
         --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

      aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
        --stack-name $FUNCTIONDASHED --capabilities CAPABILITY_NAMED_IAM
    done


Upload Essentials Configurations
--------------------------------
sosw-managed Lambdas (and Essentials themselves) will automatically try to read their
configuration from the DynamoDB table ``config``. Each Lambda looks for the document with
a range_key ``config_name = 'LAMBDA_NAME_config'`` (e.g. ``'sosw_orchestrator_config'``).

The ``config_value`` should contain a JSON that will be recursively merged to the
``DEFAULT_CONFIG`` of each Lambda.

We have provided some very basic examples of configuring Essentials.
The config files have some values that are dependant on your AWS Account ID,
so we shall substitute it and then upload these configs to DynamoDB.
It is much easier to do this in Python, so we shall call a python script for that. The
script uses some `sosw` features for working with DynamoDB, so we shall have to install sosw.

..  code-block:: bash

    cd /var/app/sosw
    pipenv run pip install sosw
    cd /var/app/sosw/examples/
    pipenv run python3 config_updater.py

    ### Or alternatively use old one:
    # cd /var/app/sosw/examples/essentials/.config
    # python3 config_uploader.py
    # cd /var/app/sosw

Please take your time to read more about :ref:`Config Sourse<Config_Sourse>` and find
advanced examples in the guidelines of :ref:`Orchestrator`, :ref:`Scavenger`
and :ref:`Scheduler`.


Create Scheduled Rules
----------------------
The usual implementation expects the ``Orchestrator`` and ``Scavenger`` to run every minute,
while ``Scheduler`` and ``WorkerAssistant`` are executed per request. ``Scheduler`` may have
any number of cronned Business Tasks with any desired periodicity of course.

The following script will create an AWS CloudWatch Events Scheduled Rule that will invoke
the ``Orchestrator`` and ``Scavenger`` every minute.

..  note::

    Make sure not to leave this rule enabled after you finish your tutorial, because after
    passing the free tier of AWS for Lambda functions it might cause unexpected charges.

..  code-block:: bash

    # Set parameters:
    BUCKETNAME=sosw-s3-$ACCOUNT
    PREFIX=/var/app/sosw/examples/yaml
    FILENAME=sosw-dev-scheduled-rules.yaml
    STACK=sosw-dev-scheduled-rules

    aws cloudformation package --template-file $PREFIX/$FILENAME \
        --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

    aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
        --stack-name $STACK --capabilities CAPABILITY_NAMED_IAM

..  hidden-code-block:: bash
    :label: Manual creation of rules <br>

    ############
    # WARNING! #
    ############
    # This is not recommended to run!
    # Use CloudFormation.

    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
        grep AccountId | awk -F "\"" '{print $4}'`

    aws events put-rule --schedule-expression 'rate(1 minute)' --name SoswEssentials
    aws events put-targets --rule SoswEssentials \
        --targets \
            "Id"="1","Arn"="arn:aws:lambda:us-west-2:$ACCOUNT:function:sosw_orchestrator" \
            "Id"="2","Arn"="arn:aws:lambda:us-west-2:$ACCOUNT:function:sosw_scavenger"
