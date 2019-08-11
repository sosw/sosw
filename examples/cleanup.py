"""
Cleanup
===============
"""

import json
import os
import re
import sys

from pathlib import Path
from urllib import request
from sosw import Processor
from sosw.components.helpers import recursive_update


DEFAULT_CONFIGS = 'essentials/.config'


class Cleanup(Processor):
    DEFAULT_CONFIG = {
        "init_clients": ["cloudformation"]
    }

    cloudformation_client = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Indentify your AWS Account ID from instance meta. Assuming you are still running this on EC2 instance.
        r = request.urlopen('http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info/')
        meta = json.loads(r.read().decode('utf-8'))
        self.aws_account = meta['AccountId']

        self.env = kwargs.get('env', 'production')


    def get_stacks(self):
        data = self.cloudformation_client.list_stacks()['StackSummaries']
        print(data)
        return [x for x in data if x['StackName'].startswith('sosw-')]


    def __call__(self, *args, **kwargs):
        stacks = self.get_stacks()
        print(stacks)

        for stack in stacks:
            self.cloudformation_client.delete_stack(StackName=stack['StackName'])


if __name__ == "__main__":
    processor = Cleanup()
    processor()
