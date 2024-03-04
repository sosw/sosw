#!/usr/bin/env bash
set -e

# Change ACCOUNT_ID and S3 bucket name appropriately.
NAME=sosw
PROFILE=default

HELPMSG="USAGE: ./deploy.sh [-p profile]
Deploys sosw layer. Installs sosw from latest pip version.\n
Use -p in case you have specific profile (not the default one) in you .aws/config with appropriate permissions."

while getopts ":p:fh" option
do
    case "$option"
    in
        p) PROFILE=$OPTARG;;
        f) FORCE_RANDOM_NAME=true;;
        h|*) echo -e "$HELPMSG";exit;;
    esac
done

ACCOUNT_ID=`aws sts get-caller-identity --query "Account" --output text --profile $PROFILE`
#FIXME SHOULD BE THIS BUCKET_NAME="sosw-s3-$ACCOUNT_ID"
BUCKET_NAME=s3-control-bucket-$ACCOUNT_ID

# Install package with respect to the rule of Lambda Layers:
# https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html

pip3 install $NAME --no-dependencies -t $NAME/python/

# Package other (non-sosw) reqired libraries
pip3 install aws_lambda_powertools -t $NAME/python/
pip3 install aws_xray_sdk -t $NAME/python/
pip3 install bson --no-dependencies -t $NAME/python/
pip3 install requests -t $NAME/python/

if [[ $FORCE_RANDOM_NAME ]]; then
  echo "Generated a random suffix for file name."
  FILE_NAME=$NAME-$RANDOM
else
  FILE_NAME=$NAME
fi

zip_path="/tmp/$FILE_NAME.zip"
stack_name="layer-$NAME"

echo "Packaging..."
if [ -f "$zip_path" ]
then
    rm $zip_path
    echo "Removed the old package."
fi


cd $NAME
zip -qr $zip_path *
cd ..
echo "Created a new package in $zip_path."

aws s3 cp $zip_path s3://$BUCKET_NAME/lambda_layers/ --profile $PROFILE
echo "Uploaded $zip_path to S3 bucket to $BUCKET_NAME."

if test -z "$ENVIRONMENT"
then
  env_name="prod"
else
  env_name=$ENVIRONMENT
fi

echo "Deploying stack $stack_name with CloudFormation for environment ${env_name}"
aws cloudformation package --template-file $NAME.yaml --output-template-file deployment-output.yaml \
    --s3-bucket $BUCKET_NAME --profile $PROFILE
echo "Created package from CloudFormation template"

echo "Calling for CloudFormation to deploy"
aws cloudformation deploy --template-file ./deployment-output.yaml --stack-name $stack_name \
    --parameter-overrides EnvironmentName=$env_name FileName=$FILE_NAME.zip \
    --capabilities CAPABILITY_NAMED_IAM --profile $PROFILE

echo "Finished with stack $stack_name. If there were changes in the YAML, they should be applied."
