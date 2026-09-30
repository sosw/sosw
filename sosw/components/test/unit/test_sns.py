import boto3
import json
import logging
import shutil
import unittest
import uuid
import os
import csv

from collections import defaultdict
from unittest.mock import MagicMock, patch
from sosw.components.sns import *


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class sns_TestCase(unittest.TestCase):

    def clean_queue(self):
        setattr(self.sns, 'queue', [])


    def setUp(self):
        self.sns = SnsManager(test=True, subject='Autotest SNS Subject')
        self.sns.commit = MagicMock(side_effect=self.clean_queue)

    def tearDown(self):
        pass


    def test_init__reads_config(self):

        sns = SnsManager(config={'subject': 'subj', 'recepient': 'arn::some_topic'})

        self.assertEqual(sns.recipient, 'arn:aws:sns:us-west-2:000000000000:autotest_topic',
                         "The Topic must be automatically reset for test")
        self.assertEqual(sns.subject, 'subj', "Subject was not set during __init__ from config.")


    def test_queue_message(self):
        self.sns.send_message("test message")
        self.assertEqual(len(self.sns.queue), 1, "Default send_message() did not queue the message.")


    def test_queue_message_with_subject(self):
        self.sns.send_message("test message", subject="New Subject")
        self.assertEqual(len(self.sns.queue), 1, "send_message() with custom subject did not queue.")


    def test_commit_queue(self):
        self.sns.send_message("test message")
        self.sns.commit()
        self.assertEqual(len(self.sns.queue), 0, f"Commit did not clean queue")
        self.sns.commit.assert_called_once()


    def test_commit_on_change_subject(self):
        self.sns.send_message("test message")
        self.sns.set_subject("New Subject")
        self.assertEqual(len(self.sns.queue), 0, "On change subject the queue should be committed.")


    def test_no_commit_on_change_subject_if_subject_is_same(self):
        self.sns.send_message("test message")
        self.sns.set_subject("Autotest SNS Subject")
        self.assertEqual(len(self.sns.queue), 1, "On change subject the queue should be committed.")


    def test_no_commit_on_same_subject(self):
        self.sns.send_message("test message")
        self.sns.send_message("test message", subject="Autotest SNS Subject")
        self.assertEqual(len(self.sns.queue), 2, "On sending message with exactly same subject, it should be queued.")


    def test_commit_and_queue_on_change_subject(self):
        self.sns.send_message("test message")
        self.assertEqual(len(self.sns.queue), 1)
        self.sns.send_message("test message", subject="New Subject")
        self.assertEqual(len(self.sns.queue), 1, "On change subject, old message should be committed, new one queued.")


    def test_commit_auto_on_change_recipient(self):
        self.sns.send_message("test message")
        self.assertEqual(len(self.sns.queue), 1, f"Initial send_message() did not queue the message")

        self.sns.set_recipient('arn:aws:sns:new_recipient')
        self.assertEqual(len(self.sns.queue), 0)


    def test_no_commit_on_change_recipient_if_recipient_is_same(self):
        self.sns.send_message("test message")
        self.assertEqual(len(self.sns.queue), 1, f"Initial send_message() did not queue the message")

        self.sns.set_recipient('arn:aws:sns:us-west-2:000000000000:autotest_topic')
        self.assertEqual(len(self.sns.queue), 1)


    def test_validate_recipient(self):
        """
        Must be a string with ARN of SNS Topic. Validator just checks that string starts with 'arn:aws:'
        """
        self.assertRaises(AssertionError, self.sns.set_recipient, 'just_new_recipient_not_full_arn')


    def test_create_topic_invalid_name(self):
        with self.assertRaises(RuntimeError) as exc:
            self.sns.create_topic('')

        self.assertEqual(str(exc.exception), "You passed invalid topic name")

    def test_create_topic_return_value(self):
        self.sns.client = MagicMock()
        self.sns.client.create_topic = MagicMock(return_value={'TopicArn': 'test_arn'})
        self.assertEqual(self.sns.create_topic('topic_name'), 'test_arn')


    def test_create_subscription_invalid_params(self):
        with self.assertRaises(RuntimeError) as exc:
            self.sns.create_subscription('', 'protocol', 'endpoint')

        self.assertEqual(str(exc.exception), "You must send valid topic ARN, Protocol and Endpoint to add a subscription")


    def test_get_message_attribute_validate_output(self):
        self.assertEqual(self.sns.get_message_attribute(10), {'DataType': 'Number', 'StringValue': '10'})
        self.assertEqual(self.sns.get_message_attribute(10.99), {'DataType': 'Number', 'StringValue': '10.99'})
        self.assertEqual(self.sns.get_message_attribute('Test'), {'DataType': 'String', 'StringValue': 'Test'})
        self.assertEqual(
            self.sns.get_message_attribute(['Test1', 'Test2', 'Test3']),
            {'DataType': 'String.Array', 'StringValue': json.dumps(['Test1', 'Test2', 'Test3'])}
        )


    def test_commit_on_change_message_attributes(self):
        self.sns.send_message("test message")
        self.assertEqual(len(self.sns.queue), 1, "There is 1 message in the queue.")
        self.sns.send_message("test message", message_attributes={'price': 100})
        self.assertEqual(len(self.sns.queue), 1, "On change message_attributes, old message should be committed, "
                                                 "new one queued.")
        self.sns.send_message("test message", message_attributes={'price': 100})
        self.assertEqual(len(self.sns.queue), 2, "On sending message with exactly same message_attributes, it should "
                                                 "be queued.")
        self.sns.send_message("test message", message_attributes={'price': 100, 'cancellation': True})
        self.assertEqual(len(self.sns.queue), 1, "On sending message with different message_attributes, old messages "
                                                 "should be committed. New one should be queued.")


    def test_init__explicit_test_false_wins_over_stage(self):
        """
        An explicitly passed `test=False` must win even when STAGE=test: a real boto3 session is initialized
        and the recipient is not overridden with the autotest topic.
        """

        with patch('boto3.Session') as mock_session:
            sns = SnsManager(test=False, subject='subj', recipient='arn:aws:sns:us-west-2:000000000000:real_topic')

        self.assertFalse(sns.test)
        self.assertEqual(sns.recipient, 'arn:aws:sns:us-west-2:000000000000:real_topic',
                         "Recipient must not be overridden with autotest topic in non-test mode")
        mock_session.assert_called_once_with(region_name='us-west-2')
        mock_session.return_value.client.assert_called_once_with('sns')
        self.assertIs(sns.client, mock_session.return_value.client.return_value)


    def test_init__explicit_test_true_wins_over_stage(self):
        """
        An explicitly passed `test=True` must win even when STAGE is not test: no real clients initialized.
        """

        with patch.dict(os.environ, {'STAGE': 'production'}):
            with patch('boto3.Session') as mock_session:
                sns = SnsManager(test=True, subject='subj')

        self.assertTrue(sns.test)
        self.assertEqual(sns.recipient, 'arn:aws:sns:us-west-2:000000000000:autotest_topic')
        mock_session.assert_not_called()


    def test_init__test_flag_derived_from_stage(self):
        """
        Without the explicit `test` flag the mode is derived from STAGE.
        """

        sns = SnsManager(subject='subj')
        self.assertTrue(sns.test, "STAGE=test in the environment of unit tests must derive test mode")

        with patch.dict(os.environ, {'STAGE': 'production'}):
            with patch('boto3.Session') as mock_session:
                sns = SnsManager(subject='subj', region='eu-west-1')

        self.assertFalse(sns.test)
        mock_session.assert_called_once_with(region_name='eu-west-1')


    def test_del__commits_queued_messages(self):
        self.sns.queue = ['unsent message']
        self.sns.__del__()
        self.sns.commit.assert_called_once()


    def test_del__empty_queue_not_committed(self):
        self.sns.queue = []
        self.sns.__del__()
        self.sns.commit.assert_not_called()


    def test_set_separator(self):
        self.sns.set_separator('\n---\n')
        self.assertEqual(self.sns.separator, '\n---\n')


    def test_set_separator__invalid(self):
        self.assertRaises(AssertionError, self.sns.set_separator, 42)


    def test_commit__publishes_combined_message(self):
        sns = SnsManager(test=True, subject='Autotest SNS Subject')
        sns.client = MagicMock()
        sns.queue = ['first', 'second']

        sns.commit()

        sns.client.publish.assert_called_once_with(
                TopicArn='arn:aws:sns:us-west-2:000000000000:autotest_topic',
                Subject='Autotest SNS Subject',
                Message=f"first{sns.separator}second")
        self.assertEqual(sns.queue, [], "Queue must be cleaned after commit")
        self.assertIsNone(sns.message_attributes, "MessageAttributes must be reset after commit")


    def test_commit__with_message_attributes(self):
        sns = SnsManager(test=True, subject='Autotest SNS Subject')
        sns.client = MagicMock()
        sns.queue = ['some message']
        sns.message_attributes = {'price': 100, 'tag': 'sale'}

        sns.commit()

        args, kwargs = sns.client.publish.call_args
        self.assertEqual(kwargs['MessageAttributes'],
                         {
                             'price': {'DataType': 'Number', 'StringValue': '100'},
                             'tag':   {'DataType': 'String', 'StringValue': 'sale'},
                         })
        self.assertIsNone(sns.message_attributes)


    def test_commit__empty_queue_does_not_publish(self):
        sns = SnsManager(test=True, subject='Autotest SNS Subject')
        sns.client = MagicMock()

        sns.commit()

        sns.client.publish.assert_not_called()
        self.assertEqual(sns.queue, [])


    def test_commit__raises_without_recipient(self):
        sns = SnsManager(test=True, subject='Autotest SNS Subject')
        sns.recipient = None
        sns.queue = ['some message']

        with self.assertRaises(RuntimeError) as exc:
            sns.commit()

        self.assertIn("did not specify ARN of recipient", str(exc.exception))
        sns.queue = []  # Keep the destructor quiet.


    def test_commit__raises_without_subject(self):
        sns = SnsManager(test=True)
        sns.queue = ['some message']

        with self.assertRaises(RuntimeError) as exc:
            sns.commit()

        self.assertIn("did not specify Subject", str(exc.exception))
        sns.queue = []  # Keep the destructor quiet.


    def test_get_message_attribute__unsupported_type(self):
        with self.assertRaises(TypeError) as exc:
            SnsManager.get_message_attribute(None)

        self.assertIn("Unsupported message_attribute value", str(exc.exception))


    def test_send_message__raises_without_any_subject(self):
        sns = SnsManager(test=True)

        with self.assertRaises(RuntimeError) as exc:
            sns.send_message('some message')

        self.assertIn("must have specified subject", str(exc.exception))


    def test_send_message__forse_commit__sets_missing_subject(self):
        sns = SnsManager(test=True)
        sns.commit = MagicMock(side_effect=lambda: setattr(sns, 'queue', []))

        sns.send_message('some message', subject='New Subject', forse_commit=True)

        self.assertEqual(sns.subject, 'New Subject')
        sns.commit.assert_called_once()


    def test_send_message__forse_commit__with_existing_subject(self):
        self.sns.send_message('some message', forse_commit=True)
        self.sns.commit.assert_called_once()
        self.assertEqual(self.sns.subject, 'Autotest SNS Subject')


    def test_create_subscription(self):
        self.sns.client = MagicMock()

        self.sns.create_subscription('arn:aws:sns:us-west-2:000000000000:topic', 'email', 'test@sosw.app')

        self.sns.client.subscribe.assert_called_once_with(
                TopicArn='arn:aws:sns:us-west-2:000000000000:topic',
                Protocol='email',
                Endpoint='test@sosw.app')


if __name__ == '__main__':
    unittest.main()
