"""tutorial_pull_tweeter_hashtags
"""

import datetime
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
        },
        "api_valid_params": ["term", "raw_query", "geocode", "since_id", "max_id", "until", "since", "count",
                             "lang", "locale", "result_type", "include_entities", "return_json"],
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

        # Constructing variables for period of search. Default: yesterday
        until = validate_datetime_from_something(event.pop('end_date', datetime.date.today()))
        since = validate_datetime_from_something(event.pop('start_date', (until - datetime.timedelta(days=1))))
        assert since < until, f"Invalid dates. `start_date` must be before `end_date`: {since} is after {until}"

        # Concatenate words in a string and insert hashtag if missing. This is actual search query for twitter.
        words = event.pop('words')
        term = " ".join([w if w.startswith('#') else f"#{w}" for w in words])

        # Variables for twitter API request.
        query = {
            "since":            since.strftime('%Y-%m-%d'),
            "until":            until.strftime('%Y-%m-%d'),
            "term":             term,
            "count":            100,  # Maximum value for pagination
            "lang":             "en",  # Filter only tweets in English
            "result_type":      "recent",  # Order by date so that we can paginate
            "include_entities": False
        }

        # Inject the rest of event as optional args.
        query = {**query, **{k: v for k, v in event.items() if k in self.config['api_valid_params']}}
        logger.info(query)

        if not self.api:
            logger.warning(f"No twitter API client. Skipping this invocation.")
            super().__call__(event)
            return

        last_id = 0  # Placeholder for pagination of twitter requests
        while True:
            if last_id:
                query["max_id"] = last_id

            data = self.api.GetSearch(**query)

            for row in data:
                logger.debug(row)

            self.stats['tweets_pulled'] += len(data)
            if len(data) == query['count']:
                last_id = data[-1].id
                logger.info(f"last_id: {last_id}")
                continue

            # If fetched everything - break
            break

        # Prepare the data to write to DynamoDB
        row = dict(tag_name=query['term'], count=self.stats['tweets_pulled'])

        # Inject date timestamps into row.
        for k in ['since', 'until']:
            row[k] = datetime.datetime.strptime(query[k], '%Y-%m-%d').timestamp()

        self.dynamo_db_client.put(row)

        super().__call__(event)


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
