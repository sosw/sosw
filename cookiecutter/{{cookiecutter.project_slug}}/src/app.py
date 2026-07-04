"""
{{ cookiecutter.function_name }}
{{ '=' * (cookiecutter.function_name | length) }}

{{ cookiecutter.description }}

Built on the ``sosw`` framework: https://docs.sosw.app
"""

try:
    from aws_lambda_powertools import Logger

    logger = Logger()

except ImportError:
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

{% if cookiecutter.use_lambda_api == 'yes' %}
from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.components.exceptions import NotFoundError
from sosw.lambda_api import LambdaApi


class Processor(LambdaApi):
    """
    HTTP API Lambda behind API Gateway (REST v1 or HTTP v2 proxy events).

    ``LambdaApi`` dispatches requests declared in the ``routes`` config to the methods below,
    validates Cognito claims injected by the API Gateway authorizer (``auth_enabled``), renders
    JSON responses with the configured CORS headers, and converts raised ``ApiError`` subclasses
    to their HTTP error envelopes.

    The final config is built in layers: ``DEFAULT_CONFIG``, recursively updated with the
    ``{{ cookiecutter.function_name }}_config`` record of the DynamoDB ``config`` table
    (if present), recursively updated with the ``custom_config`` of the constructor.
    """

    DEFAULT_CONFIG = {
        'auth_enabled': True,
        'routes':       {
            'GET /health':            'get_health',
            'GET /things/{thing_id}': 'get_thing',
        },
        'things':       {
            '42': 'The Answer',
        },
    }


    def get_health(self, event, **kwargs):
        """
        Health check route. Requires a valid caller when ``auth_enabled`` is on.

        :param dict event:  Raw API Gateway proxy event.
        :rtype:             dict
        """

        logger.debug("Health check called by: %s", self.claims.get('sub', 'anonymous'))

        return {'status': 'ok'}


    def get_thing(self, event, thing_id=None, **kwargs):
        """
        Return a single thing by the ``thing_id`` path parameter.

        Things live in the ``things`` config parameter for this scaffold - replace with real
        storage (e.g. ``self.get_ddbc('things')`` backed by a DynamoDB table).

        :param dict event:          Raw API Gateway proxy event.
        :param str thing_id:        Path parameter extracted from the matched route.
        :rtype:                     dict
        :raises NotFoundError:      When the thing does not exist -> HTTP 404.
        """

        things = self._c('things') or {}

        if thing_id not in things:
            raise NotFoundError(f"Thing {thing_id} does not exist")

        return {'thing_id': thing_id, 'thing': things[thing_id]}
{% else %}
from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.app import Processor as SoswProcessor


class Processor(SoswProcessor):
    """
    Main worker of ``{{ cookiecutter.function_name }}``.

    The final config is built in layers: ``DEFAULT_CONFIG``, recursively updated with the
    ``{{ cookiecutter.function_name }}_config`` record of the DynamoDB ``config`` table
    (if present), recursively updated with the ``custom_config`` of the constructor.

    Warm-start contract: the instance is cached for the Lambda container lifetime, so
    per-invocation state belongs in ``self.result`` (reset by ``super().__call__``) and
    lifetime counters in ``self.stats``. Never accumulate per-event state on ``self``.
    """

    DEFAULT_CONFIG = {
        'init_clients': [],
    }


    def __call__(self, event):
        super().__call__(event)

        records = event.get('records', [])
        for record in records:
            self.process_record(record)

        self.result['processed'] = len(records)

        return dict(self.result)


    def process_record(self, record):
        """
        Process a single record. Replace with the real business logic.

        :param record:  Element of the ``records`` list of the event.
        """

        logger.info("Processing record: %s", record)
        self.stats['records_processed'] += 1
{% endif %}


global_vars = LambdaGlobals()
lambda_handler = get_lambda_handler(Processor, global_vars)
