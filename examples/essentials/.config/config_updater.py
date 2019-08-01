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
from sosw import Processor
from sosw.components.helpers import recursive_update


class ConfigUploader(Processor):
    DEFAULT_CONFIG = {
        'init_clients':     ['DynamoDb'],
        'dynamo_db_config': {
            'table_name': 'config',
            'row_mapper': {
                'env':         'S',
                'config_name': 'S',
            }
        }
    }

    dynamo_db_client = None


    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # Indentify your AWS Account ID from instance meta. Assuming you are still running this on EC2 instance.
        r = request.urlopen('http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/')
        meta = json.loads(r.read().decode('utf-8'))
        self.aws_account = meta['AccountId']

        self.env = kwargs.get('env', 'production')


    def upload_from_files(self):
        """
        | Takes JSON files in current directory and uploads them to config table.
        | The 12-zeros pattern is replaces with current AWS Account ID.
        """
        for entry in os.scandir('.'):

            if entry.is_file() and entry.path.endswith('json'):
                # Take just the name from file.
                config_name = entry.path[2:-5]

                # Clean out of extra word separators.
                config_value = re.sub('\s+', '', Path(entry.path).read_text())

                # Substitute your account
                config_value = config_value.replace('000000000000', self.aws_account)

                with open(entry.path, 'r') as f:
                    data = {
                        'env':          {'S': self.env},
                        'config_name':  {'S': config_name},
                        'config_value': {'S': config_value}
                    }

                print(f"Uploading {data}")
                self.dynamo_db_client.put(row=data)


    def get_config(self, name):
        """
        Return the value of config with given `name`.
        """
        result = self.dynamo_db_client.get_by_query({'env': self.env, 'config_name': name})
        assert len(result) == 1
        return result[0]['config_value']


    def update_config(self, name, value):
        """
        Insert the new `value` in the config with `name` recursively respecting the current values.
        """
        current = self.get_config(name)
        new = recursive_update(current, value)

        self.dynamo_db_client.put(row=new, overwrite_existing=True)


if __name__ == "__main__":
    uploader = ConfigUploader()
    uploader.upload_from_files()