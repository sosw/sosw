SNS_EVENT = {
    'Records': [{
        'EventSource':          'aws:sns',
        'EventSubscriptionArn': 'arn:aws:sns:us-west-2:000:some_sns:xyz',
        'Sns':                  {
            'Type':      'Notification',
            'MessageId': '111',
            'TopicArn':  'arn:aws:sns:us-west-2:000:some_topic',
            'Subject':   'You have some message',
            'Message':   '{"hello": "I am Inigo Montoya"}',
        }
    }]
}

SNS_NESTED = {
    'Message':   '{"hello": "I am Inigo Montoya"}',
    'MessageId': '111',
    'TopicArn':  'arn:aws:sns:us-west-2:000:some_sns',
    'Type':      'Notification',
}

SQS_EVENT = {
    'Records': [{
        'body':           '{"hello": "I am Inigo Montoya"}',
        'eventSource':    'aws:sqs',
        'eventSourceARN': 'arn:aws:sqs:us-west-2:000:some_queue',
        'messageId':      '83760',
        'receiptHandle':  'fgledl9494'
    }]
}

EVENT_SNS_INSIDE_SQS = {
    'Records': [{
        'messageId':      '111',
        'receiptHandle':  'abc123',
        'body':           '{"Message": "{\\"hello\\": \\"I am Inigo Montoya\\"}", "MessageId": "111", "TopicArn": "arn:aws:sns:us-west-2:000:some_sns", "Type": "Notification"}',
        'eventSource':    'aws:sqs',
        'eventSourceARN': 'arn:aws:sqs:us-west-2:000:some_sqs',
        'awsRegion':      'us-west-1'
    }]
}

SQS_EVENT_MANY = {
    'Records': [{
        'body':           '{"hello": "I am Inigo Montoya"}',
        'eventSource':    'aws:sqs',
        'eventSourceARN': 'arn:aws:sqs:us-west-2:000:some_queue',
        'messageId':      '83760',
        'receiptHandle':  'fgledl9494'
    }, {
        'body':           '{"hello2": "I am Inigo Montoya2"}',
        'eventSource':    'aws:sqs',
        'eventSourceARN': 'arn:aws:sqs:us-west-2:000:some_queue',
        'messageId':      '84732',
        'receiptHandle':  '85y373'
    }]
}
