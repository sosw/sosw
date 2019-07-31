"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers
    Copyright (C) 2019  sosw core contributors

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/gpl-3.0.html>.
"""

__all__ = ['SnsManager']
__author__ = "Nikolay Grishchenko"

import boto3
import csv
import json
import logging
import os
from collections import defaultdict


logger = logging.getLogger()


class SnsManager():
    """
    AWS Simple Notification System Manager helper.
    Requires the Role to have permissions to access SSM for requested resources, something like.

    Must have a recipient specified either during initialization or later using set_recipient method.
    Messages received by send_message() are batched and will be actually send to SNS only during the
    call of commit() or during the destruction of the class.

    The method doesn't yet support batching messages for multiple recipients, so in case you try to
    change the recipient it automatically sends all the currently batched messages to the previous recipient.
    """


    def __init__(self, **kwargs):
        """
        :param recipient:   - str   - ARN of the default recipient.
        :param subject:     - str   - Default subject for messages.
        """

        self.stats = defaultdict(int)
        config = kwargs.get('config', kwargs.get('custom_config', {}))
        if config:
            self.recipient = config.get('recipient')
            self.subject = config.get('subject')
        else:
            self.recipient = kwargs.get('recipient')
            self.subject = kwargs.get('subject')

        self.queue = []

        self.test = kwargs.get('test') or True if os.environ.get('STAGE') == 'test' else False
        if self.test:
            self.recipient = 'arn:aws:sns:us-west-2:000000000000:autotest_topic'

        if not self.test:
            self.session = boto3.Session(region_name=kwargs.get('region', 'us-west-2'))
            self.resource = self.session.client('sns')


    def __del__(self):
        """
        Destructor. Send unsent queued messages.
        """

        if self.queue:
            self.commit()


    def set_client_attr(self, name, value):
        """
        Sets the given _name_ attribute to _value_.
        Commits the self.queue if there are any messages in it.
        """

        if getattr(self, name) != value:
            if self.queue:
                self.commit()

            setattr(self, name, value)


    def set_recipient(self, arn):
        assert isinstance(arn, str), f"Invalid format of ARN: {arn}. Recipient must be string ARN of SNS Topic"
        assert arn.lower().startswith('arn:aws:'), f"Invalid format of ARN: {arn}. Recipient must be ARN of SNS Topic"
        self.set_client_attr('recipient', value=arn)


    def set_subject(self, value):
        self.set_client_attr('subject', value=str(value))


    def commit(self):
        """
        Combines messages from self.queue and pushes them to self.recipient.
        Cleans the queue after that.
        """

        # Check that the ARN of recipient is set.
        if not self.recipient:
            raise RuntimeError("You did not specify ARN of recipient to send message to. "
                               "Please use self.sns.set_recipient() from your Lambda __")

        # Check that the SNS subject is set.
        if not self.subject:
            raise RuntimeError("You did not specify Subject for the message. "
                               "We don't want you to write code like this, please fix.")

        message = "\n\n#####\n\n".join(self.queue)

        if message:
            self.resource.publish(
                    TopicArn=self.recipient,
                    Subject=self.subject,
                    Message=message)

        self.queue = []


    def send_message(self, message, subject=None, forse_commit=False):
        """
        If the subject is not yet set (for example during __init__() of the class) - then require subject to be set.
        Otherwize we accept None subject and simply append messages to queue.
        Once the subject changes - the queue is commite automatically.

        :param message:     - str   - Message to be send in body of SNS message. Queued.
        :param subject:     - str   - Optional. Custom subject for message.
        """

        if not any([self.subject, subject]):
            raise RuntimeError("You must have specified subject for self.sns.send_message() either "
                               "during __init__() of the class, or in the first send_message in kwargs.")

        if all([self.subject, subject]) and not self.subject == subject:
            logger.info("Change of subject detected. We commit (send) the current queue.")
            self.set_subject(subject)  # This will also commit existing messages automatically.
            self.queue = [message]
        else:
            self.queue.append(message)

        if forse_commit:
            if subject and not self.subject:
                self.subject = subject
            logger.info("The caller asked to forse_commit, so we commit the queue immediately.")
            self.commit()
