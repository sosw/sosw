"""Config uploader.

Reads the config files from current directory and saves them to the ``config`` table in DynamoDB.

The file is supposed to be executed from the directory with configs and on some EC2 machine in the same account
as DynamoDB you are writing to. The AccountId is then substituted to the placeholders in configs.
Safe against multiple runs, will simply overwrite configs.
"""

import boto3
import json
import os
import re

from pathlib import Path
from urllib import request


# Indentify your AWS Account ID from instance meta. Assuming you are still running this on EC2 instance.
r = request.urlopen('http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/')
meta = json.loads(r.read().decode('utf-8'))
account_id = meta['AccountId']

client = boto3.client('dynamodb')

for entry in os.scandir('.'):

    if entry.is_file() and entry.path.endswith('json'):

        # Take just the name from file.
        config_name = entry.path[2:-5]

        # Clean out of extra word separators.
        config_value = re.sub('\s+', '', Path(entry.path).read_text())

        # Substitute your account
        config_value = config_value.replace('000000000000', account_id)

        with open(entry.path, 'r') as f:
            data = {
                'env':          {'S': 'production'},
                'config_name':  {'S': config_name},
                'config_value': {'S': config_value}
            }

        print(f"Uploading {data}")
        client.put_item(TableName='config', Item=data)
