"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2019  sosw core contributors <info@sosw.app>

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
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
        :param str recipient: ARN of the default recipient.
        :param str subject: Default subject for messages.
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
        self.separator = "\n\n#####\n\n"

        self.test = kwargs.get('test') or True if os.environ.get('STAGE') == 'test' else False
        if self.test:
            self.recipient = 'arn:aws:sns:us-west-2:000000000000:autotest_topic'

        if not self.test:
            self.session = boto3.Session(region_name=kwargs.get('region', 'us-west-2'))
            self.client = self.session.client('sns')


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


    def set_separator(self, separator):
        """
        Set custom separator for messages from the queue
        """

        assert isinstance(separator, str), f"Invalid format of separator: {separator}. Separator must be string."
        setattr(self, 'separator', separator)


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

        message = self.separator.join(self.queue)

        if message:
            self.client.publish(
                    TopicArn=self.recipient,
                    Subject=self.subject,
                    Message=message)

        self.queue = []


    def send_message(self, message, subject=None, forse_commit=False):
        """
        If the subject is not yet set (for example during __init__() of the class) - then require subject to be set.
        Otherwize we accept None subject and simply append messages to queue.
        Once the subject changes - the queue is commite automatically.

        :param str message: Message to be send in body of SNS message. Queued.
        :param str subject: Optional. Custom subject for message.
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


    def create_topic(self, topic_name):
        """
        Create a new topic name

        :param str topic_name: New topic name to create
        :return: New topic ARN
        :rtype: str
        """

        if not topic_name or not isinstance(topic_name, str):
            raise RuntimeError("You passed invalid topic name")

        topic = self.client.create_topic(Name=topic_name)

        return topic.get('TopicArn')


    def create_subscription(self, topic_arn, protocol, endpoint):
        """
        Create a subscription to the topic

        :param str topic_arn: ARN of a topic
        :param str protocol: The type of endpoint to subscribe
        :param str endpoint: Endpoint that can receive notifications from Amazon SNS
        """

        if not all([topic_arn, protocol, endpoint]):
            raise RuntimeError("You must send valid topic ARN, Protocol and Endpoint to add a subscription")

        self.client.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint
        )
