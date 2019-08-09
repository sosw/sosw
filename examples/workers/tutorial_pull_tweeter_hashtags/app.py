"""tutorial_pull_tweeter_hashtags
"""

import datetime
import dateutil
import logging
import twitter

from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import validate_datetime_from_something
from sosw.worker import Worker


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Processor(Worker):
    DEFAULT_CONFIG = {
        "init_clients":     ["DynamoDb"],
        "dynamo_db_config": {
            'table_name':      'tutorial_pull_tweeter_hashtags',
            'row_mapper':      {
                'tag_name': 'S',
                'since':    'N',
                'until':    'N',
                'count':    'N',
            },
            'required_fields': ['tag_name', 'since'],
        }
    }

    dynamo_db_client: DynamoDbClient = None


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            self.api = twitter.Api(**self.config['credentials'])
        except Exception as err:
            logger.exception(err)
            self.api = None
            logger.warning(f"Failed to initialize twitter API. Probably credentials missing in config: {self.config}")


    def __call__(self, event):

        until = validate_datetime_from_something(event.pop('start_date', datetime.date.today()))
        vars = {
            "since":            (until - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
            "until":            until.strftime('%Y-%m-%d'),
            "term":             event.pop('words')[0],
            "count":            100,
            "lang":             "en",
            "result_type":      "recent",
            "include_entities": False
        }

        # Inject the rest of event as optional args.
        # No validation here, so never use this kind of code in Production!
        vars = {**vars, **event}
        logger.info(vars)

        if not self.api:
            logger.warning(f"No twitter API client. Skipping this invocation.")
            super().__call__(event)
            return

        last_id = 0
        while True:
            if last_id:
                vars["max_id"] = last_id

            data = self.api.GetSearch(**vars)

            for row in data:
                logger.debug(row)

            self.stats['tweets_pulled'] += len(data)
            if len(data) == vars['count']:
                last_id = data[-1].id
                logger.info(f"last_id: {last_id}")
                continue

            # If fetched everything - break
            break

        # Prepare the data to write to DynamoDB
        row = dict(tag_name=vars['term'], count=self.stats['tweets_pulled'])

        # Inject date timestamps into row.
        for k in ['since', 'until']:
            row[k] = vars[k].timestamp() if isinstance(vars[k], datetime.datetime) \
                else dateutil.parser.parse(vars[k]).timestamp()

        self.dynamo_db_client.put(row)

        super().__call__(event)


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
