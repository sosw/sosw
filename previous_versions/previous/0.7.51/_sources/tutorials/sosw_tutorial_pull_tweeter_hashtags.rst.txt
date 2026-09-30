Tutorial Pull Tweeter Hashtags
==============================

In this tutorial we are going to pull the data about popularity of several different topics in the hash tags of tweets.
Imagine that we have already classified some popular hashtags to several specific groups. Now we want to pull the data
about their usage in the last day. 

Every day we would want to read the data only for the last 3 previous days, so we add the chunking parameters:
``"period": "last_2_days", "isolate_days": true``

We shall pretend that pulling the data for every keyword takes several minutes. 
In this case we add another parameter: ``“isolate_words”: true`` to make sure that each word shall be processed
in an different Lambda execution.

The following payload shall become our Job.
This means that we shall chunk it per specific word / day combination and create independent tasks for the
Worker Lambda for each chunk.

.. code-block:: json

   {
     "topics": {
       "cars": {
         "words": ["toyota", "mazda", "nissan", "racing", "automobile", "car"]
       },
       "food": {
         "words": ["recipe", "cooking", "eating", "food", "meal", "tasty"]
       },
       "shows": {
         "words": ["opera", "cinema", "movie", "concert", "show", "musical"]
       }
     },
     "period": "last_2_days",
     "isolate_days": true,
     "isolate_words": true
   }


Now the time has come to create the actual Lambda.

Register Twitter App
--------------------
| First of all you will have to register your own twitter API credentials. https://developer.twitter.com/
| Submitting the application takes 2-3 minutes, but after that you have to wait severals hours (or even days).
  You can submit the application now and add them to config later. The Lambda shall handle the missing credentials.

Package Lambda Code
-------------------

Creating the Lambda is very similar to the way we deployed ``sosw`` Essentials. We use the same scripts and deployment
workflow. Feel free to use your own favourite method or contribute to upgrade this one.

..  code-block:: bash

    # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
        grep AccountId | awk -F "\"" '{print $4}'`

    # Set your bucket name
    BUCKETNAME=sosw-s3-$ACCOUNT

    FUNCTION="sosw_tutorial_pull_tweeter_hashtags"
    FUNCTIONDASHED=`echo $FUNCTION | sed s/_/-/g`

    cd /var/app/sosw/examples/workers/$FUNCTION

    # Install sosw package locally. It's only dependency is boto3, but we have it in Lambda
    # containter already. Saving a lot of packages size ignoring this dependency.
    # Install other possible requirements directly into package.
    pip3 install sosw --no-dependencies --target .
    pip3 install -r requirements.txt --target .

    # Make a source package. TODO is skip 'dist-info' and 'test' paths.
    zip -qr /tmp/$FUNCTION.zip *

    # Upload the file to S3, so that AWS Lambda will be able to easily take it from there.
    aws s3 cp /tmp/$FUNCTION.zip s3://$BUCKETNAME/sosw/packages/

    # Package and Deploy CloudFormation stack for the Function.
    # It will create the Function and a custom IAM role for it with permissions to
    # acces the required DynamoDB tables.
    aws cloudformation package --template-file $FUNCTION.yaml \
        --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

    aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
        --stack-name $FUNCTIONDASHED --capabilities CAPABILITY_NAMED_IAM

This pattern has created the IAM Role for the function, the Lambda function itself and a
DynamoDB table to save data to. All these resources are still falling under the AWS free tier
if you do not abuse them.

In case you will later make any changes to the application and need to re-deploy
a new version, you may use the following script. It will validate changes in CloudFormation
template and also publish the new version of the Lambda code package:

..  hidden-code-block:: bash
    :label: Show script <br>

    # Get your AccountId from EC2 metadata. Assuming you run this on EC2.
    ACCOUNT=`curl http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/ | \
        grep AccountId | awk -F "\"" '{print $4}'`

    # Set your bucket name
    BUCKETNAME=sosw-s3-$ACCOUNT

    FUNCTION="sosw_tutorial_pull_tweeter_hashtags"
    FUNCTIONDASHED=`echo $FUNCTION | sed s/_/-/g`

    cd /var/app/sosw/examples/workers/$FUNCTION

    # Make a source package.
    zip -qr /tmp/$FUNCTION.zip *

    # Upload the file to S3, so that AWS Lambda will be able to easily take it from there.
    aws s3 cp /tmp/$FUNCTION.zip s3://$BUCKETNAME/sosw/packages/

    aws cloudformation package --template-file $FUNCTION.yaml \
      --output-template-file /tmp/deployment-output.yaml --s3-bucket $BUCKETNAME

    aws cloudformation deploy --template-file /tmp/deployment-output.yaml \
        --stack-name $FUNCTIONDASHED --capabilities CAPABILITY_NAMED_IAM

    aws lambda update-function-code --function-name $FUNCTION --s3-bucket $BUCKETNAME \
        --s3-key sosw/packages/$FUNCTION.zip --publish

Upload configs
--------------
In order for this function to be managed by ``sosw``, we have to register in as a Labourer
in the configs of sosw-Essentials. As you probably remember the configs are in the
``config`` DynamoDB table.

Specially for this tutorial we have a nice script to inject configs. It finds the JSON files
of the worker in ``FUNCTION/config`` and *"injects"* the ``labourer.json`` contents to the
existing configs of Essentials. It will also create a config for the Worker Lambda itself
out of the ``self.json``. You shall add twitter credentials in the placeholders there once
you receive them and re-run the uploader.

..  code-block:: bash

    cd /var/app/sosw/examples
    pipenv run python3 config_updater.py sosw_tutorial_pull_tweeter_hashtags

After updating the configs we must reset the Essentials so that they read fresh configs from
the DynamoDB. There is currently no special AWS API endpoint for this, so we just re-deploy
the essentials.

..  code-block:: bash

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
    done


Schedule task
-------------
| Congratulations!
| You are ready to **run** the tutorial. You just to call the ``sosw_scheduler`` Lambda
  with the Job that we constructed at the very beginning. The payload for the Scheduler
  must have the ``labourer_id`` which is the name of Worker function and the optional ``job``.

..  hidden-code-block:: json
    :label: See full payload <br>

    {
      "lambda_name": "sosw_tutorial_pull_tweeter_hashtags",
      "job": {
        "topics": {
          "cars": {
            "isolate_words": true,
            "words": ["toyota", "mazda", "nissan", "racing", "automobile", "car"]
          },
          "food": {
            "isolate_words": true,
            "words": ["recipe", "cooking", "eating", "food", "meal", "tasty"]
          },
          "shows": {
            "isolate_words": true,
            "words": ["opera", "cinema", "movie", "concert", "show", "musical"]
          }
        },
        "period": "last_2_days",
        "isolate_days": true,
        "isolate_words": true
      }
    }

This JSON payload is also available in the file ``FUNCTION/config/task.json``.

..  code-block:: bash

    cd /var/app/sosw/examples
    PAYLOAD=`cat workers/sosw_tutorial_pull_tweeter_hashtags/config/task.json`
    aws lambda invoke --function-name sosw_scheduler \
        --payload "$PAYLOAD" /tmp/output.txt && cat /tmp/output.txt

