import datetime

from dateutil.tz import tzlocal

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

PPR_DESCRIBE_TABLE = {
    'Table': {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'},
            {'AttributeName': 'session', 'AttributeType': 'S'},
            {'AttributeName': 'session_id', 'AttributeType': 'S'},
        ],
        'TableName': 'actions',
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'TableStatus': 'ACTIVE',
        'CreationDateTime': datetime.datetime(2019, 8, 22, 12, 46, 57, 555000, tzinfo=tzlocal()),
        'ProvisionedThroughput': {
            'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 36, 53, 843000, tzinfo=tzlocal()),
            'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 14, 5, 50, 889000, tzinfo=tzlocal()),
            'NumberOfDecreasesToday': 0,
            'ReadCapacityUnits': 0,
            'WriteCapacityUnits': 0
        },
        'TableSizeBytes': 4096,
        'ItemCount': 1000,
        'TableArn': 'arn:aws:dynamodb:REGION:ID:table/actions',
        'TableId': '11111111111111111111111111111111',
        'BillingModeSummary': {
            'BillingMode': 'PAY_PER_REQUEST',
            'LastUpdateToPayPerRequestDateTime': datetime.datetime(2020, 7, 5, 15, 25, 17,52000, tzinfo=tzlocal())
        },
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'session',
                'KeySchema': [{'AttributeName': 'session', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'IndexStatus': 'ACTIVE',
                'ProvisionedThroughput': {
                    'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 35, 34, 237000, tzinfo=tzlocal()),
                    'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 15, 26, 49, 920000, tzinfo=tzlocal()),
                    'NumberOfDecreasesToday': 0,
                    'ReadCapacityUnits': 0,
                    'WriteCapacityUnits': 0
                },
                'IndexSizeBytes': 2048,
                'ItemCount': 1000,
                'IndexArn': 'arn:aws:dynamodb:REGION:ID:table/actions/index/session'
            },
            {
                'IndexName': 'session_id',
                'KeySchema': [{'AttributeName': 'session_id', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'IndexStatus': 'ACTIVE',
                'ProvisionedThroughput': {
                    'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 35, 31, 516000, tzinfo=tzlocal()),
                    'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 15, 22, 32, 292000, tzinfo=tzlocal()),
                    'NumberOfDecreasesToday': 0,
                    'ReadCapacityUnits': 0,
                    'WriteCapacityUnits': 0
                },
                'IndexSizeBytes': 2048,
                'ItemCount': 1000,
                'IndexArn': 'arn:aws:dynamodb:REGION:ID:table/actions/index/session_id'
            },
        ],
        'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        'LatestStreamLabel': '2019-08-22T12:46:57.555',
        'LatestStreamArn': 'arn:aws:dynamodb:REGION:ID:table/actions/stream/DATE'
    },
    'ResponseMetadata': {
            'RequestId': 'A11A1A1AA11A1A1A11A11A11111A1A11A1A1A1A',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Server',
            'date': 'Tue, 09 Mar 2021 18:38:40 GMT',
            'content-type': 'application/x-amz-json-1.0',
            'content-length': '3379',
            'connection': 'keep-alive',
            'x-amzn-requestid': '111111111111111',
            'x-amz-crc32': '11111111111'
        },
        'RetryAttempts': 0
    }
}

PT_DESCRIBE_TABLE = {
    'Table': {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'},
            {'AttributeName': 'name', 'AttributeType': 'S'},
            {'AttributeName': 'city', 'AttributeType': 'S'},
        ],
        'TableName': 'partners',
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'TableStatus': 'ACTIVE',
        'CreationDateTime': datetime.datetime(2019, 8, 22, 12, 46, 57, 555000, tzinfo=tzlocal()),
        'ProvisionedThroughput': {
            'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 36, 53, 843000, tzinfo=tzlocal()),
            'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 14, 5, 50, 889000, tzinfo=tzlocal()),
            'NumberOfDecreasesToday': 0,
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 10
        },
        'TableSizeBytes': 4096,
        'ItemCount': 1000,
        'TableArn': 'arn:aws:dynamodb:REGION:ID:table/partners',
        'TableId': '11111111111111111111111111111111',
        'BillingModeSummary': {
            'BillingMode': 'PROVISIONED',
            'LastUpdateToPayPerRequestDateTime': datetime.datetime(2020, 7, 5, 15, 25, 17,52000, tzinfo=tzlocal())
        },
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'name',
                'KeySchema': [{'AttributeName': 'name', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'IndexStatus': 'ACTIVE',
                'ProvisionedThroughput': {
                    'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 35, 34, 237000, tzinfo=tzlocal()),
                    'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 15, 26, 49, 920000, tzinfo=tzlocal()),
                    'NumberOfDecreasesToday': 0,
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 10
                },
                'IndexSizeBytes': 2048,
                'ItemCount': 1000,
                'IndexArn': 'arn:aws:dynamodb:REGION:ID:table/partners/index/session'
            },
            {
                'IndexName': 'city',
                'KeySchema': [{'AttributeName': 'city', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'IndexStatus': 'ACTIVE',
                'ProvisionedThroughput': {
                    'LastIncreaseDateTime': datetime.datetime(2020, 7, 5, 13, 35, 31, 516000, tzinfo=tzlocal()),
                    'LastDecreaseDateTime': datetime.datetime(2020, 7, 5, 15, 22, 32, 292000, tzinfo=tzlocal()),
                    'NumberOfDecreasesToday': 0,
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 10
                },
                'IndexSizeBytes': 2048,
                'ItemCount': 1000,
                'IndexArn': 'arn:aws:dynamodb:REGION:ID:table/partners/index/session_id'
            },
        ],
        'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        'LatestStreamLabel': '2019-08-22T12:46:57.555',
        'LatestStreamArn': 'arn:aws:dynamodb:REGION:ID:table/partners/stream/DATE'
    },
    'ResponseMetadata': {
            'RequestId': 'A11A1A1AA11A1A1A11A11A11111A1A11A1A1A1A',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Server',
            'date': 'Tue, 09 Mar 2021 18:38:40 GMT',
            'content-type': 'application/x-amz-json-1.0',
            'content-length': '3379',
            'connection': 'keep-alive',
            'x-amzn-requestid': '111111111111111',
            'x-amz-crc32': '11111111111'
        },
        'RetryAttempts': 0
    }
}
