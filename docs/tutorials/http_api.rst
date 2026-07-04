.. _HTTP API tutorial:

========================
HTTP API with LambdaApi
========================

In this tutorial you will serve a small REST-ish API — ``/things`` — from a single Lambda behind
an **API Gateway HTTP API**, using :doc:`LambdaApi <../lambda_api>`. You will deploy it open
first, verify the routing and the error contract with ``curl``, then switch on Cognito JWT
authorization with one config change.

Prerequisites: the same as in :doc:`first_lambda` (AWS account, SAM CLI, Python 3.10+).


1. Project layout
-----------------

..  code-block:: text

    things-api/
    ├── src/
    │   ├── app.py
    │   └── requirements.txt    # sosw
    ├── tests/
    │   └── test_app.py
    └── template.yaml


2. The API Processor
--------------------

Create ``src/app.py``. The whole API surface is the ``routes`` dict; handlers are plain methods:

..  code-block:: python

    """things-api: a minimal HTTP API on sosw.lambda_api.LambdaApi."""

    import json

    from sosw.app import LambdaGlobals, get_lambda_handler
    from sosw.components.exceptions import BadRequestError, NotFoundError
    from sosw.lambda_api import LambdaApi

    # The demo "database". In a real function: DynamoDB via `self.get_ddbc('things')`.
    THINGS = {
        '1': {'thing_id': '1', 'name': 'red'},
        '2': {'thing_id': '2', 'name': 'green'},
    }


    class Processor(LambdaApi):

        DEFAULT_CONFIG = {
            'disable_ddb_config': True,
            'auth_enabled':       False,      # Step 6 switches this on.
            'routes':             {
                'GET /things':            'list_things',
                'GET /things/count':      'count_things',
                'GET /things/{thing_id}': 'get_thing',
                'POST /things':           'create_thing',
            },
        }


        def list_things(self, event, **kwargs):
            return {'things': sorted(THINGS.values(), key=lambda thing: thing['thing_id'])}


        def count_things(self, event, **kwargs):
            return {'count': len(THINGS)}


        def get_thing(self, event, thing_id=None, **kwargs):
            if thing_id not in THINGS:
                raise NotFoundError(f"Thing {thing_id} does not exist")
            return {'thing': THINGS[thing_id]}


        def create_thing(self, event, **kwargs):
            try:
                thing = json.loads(event.get('body') or '')
            except json.JSONDecodeError:
                raise BadRequestError("Request body must be valid JSON")

            if 'thing_id' not in thing:
                raise BadRequestError("Missing required field: thing_id")

            THINGS[str(thing['thing_id'])] = thing
            return self.make_response({'created': thing['thing_id']}, status_code=201)


    global_vars = LambdaGlobals()
    lambda_handler = get_lambda_handler(Processor, global_vars)

Note the route table: ``GET /things/count`` and ``GET /things/{thing_id}`` coexist —
``LambdaApi`` matches static routes before parametrized ones regardless of declaration order.


3. Unit tests with mocked events
--------------------------------

No API Gateway needed to test the full contract — handlers, routing, and error envelopes — just
call the Processor with a trimmed HTTP API (payload v2.0) event. Create ``tests/test_app.py``:

..  code-block:: python

    import json
    import os
    import sys
    import unittest

    os.environ['STAGE'] = 'test'
    os.environ['autotest'] = 'True'

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

    from app import Processor


    def make_v2_event(method='GET', path='/things', body=None, claims=None):
        """Trimmed API Gateway HTTP API (payload version 2.0) proxy event."""

        event = {
            'version':        '2.0',
            'routeKey':       f'{method} {path}',
            'rawPath':        path,
            'requestContext': {'http': {'method': method, 'path': path}},
            'body':           body,
        }
        if claims is not None:
            event['requestContext']['authorizer'] = {'jwt': {'claims': claims}}
        return event


    class ThingsApiTestCase(unittest.TestCase):

        def setUp(self):
            self.processor = Processor()


        def test_list_things(self):
            response = self.processor(make_v2_event('GET', '/things'))

            self.assertEqual(response['statusCode'], 200)
            self.assertEqual(len(json.loads(response['body'])['things']), 2)


        def test_get_thing__path_parameter(self):
            response = self.processor(make_v2_event('GET', '/things/1'))

            self.assertEqual(json.loads(response['body'])['thing']['name'], 'red')


        def test_get_thing__missing_returns_404_envelope(self):
            response = self.processor(make_v2_event('GET', '/things/999'))

            self.assertEqual(response['statusCode'], 404)
            self.assertEqual(json.loads(response['body'])['error']['code'], 'NOT_FOUND')


        def test_unknown_route_returns_404(self):
            response = self.processor(make_v2_event('DELETE', '/things/1'))

            self.assertEqual(response['statusCode'], 404)


        def test_create_thing__bad_json_returns_400(self):
            response = self.processor(make_v2_event('POST', '/things', body='not json'))

            self.assertEqual(response['statusCode'], 400)
            self.assertEqual(json.loads(response['body'])['error']['code'], 'BAD_REQUEST')


    if __name__ == '__main__':
        unittest.main()

..  code-block:: bash

    pip install sosw
    python -m unittest discover tests -v

This Processor sets ``disable_ddb_config``, so constructing it in tests needs no mocks at all.


4. The SAM template
-------------------

A single catch-all route forwards everything to the Lambda; the routing lives in your config,
not in API Gateway:

..  code-block:: yaml

    AWSTemplateFormatVersion: '2010-09-09'
    Transform: AWS::Serverless-2016-10-31
    Description: things-api - HTTP API on sosw LambdaApi.

    Resources:

      ThingsApiFunction:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: things-api
          CodeUri: src/
          Handler: app.lambda_handler
          Runtime: python3.13
          Timeout: 30
          MemorySize: 256
          Events:
            AnyRequest:
              Type: HttpApi
              Properties:
                ApiId: !Ref ThingsHttpApi
                Method: ANY
                Path: /{proxy+}

      ThingsHttpApi:
        Type: AWS::Serverless::HttpApi

    Outputs:
      ApiUrl:
        Value: !Sub 'https://${ThingsHttpApi}.execute-api.${AWS::Region}.amazonaws.com'


5. Deploy and verify the contract
---------------------------------

..  code-block:: bash

    sam build && sam deploy --guided       # stack name: things-api
    API=<ApiUrl output of the deploy>

    curl "$API/things"                      # 200: {"things": [...]}
    curl "$API/things/1"                    # 200: {"thing": {"thing_id": "1", "name": "red"}}
    curl "$API/things/count"                # 200: static route wins over {thing_id}
    curl "$API/things/999"                  # 404: {"error": {"code": "NOT_FOUND", ...}}
    curl "$API/nope"                        # 404: unknown route
    curl -X DELETE "$API/things/1"          # 404: unknown method for this path
    curl -X POST "$API/things" -d 'oops'    # 400: {"error": {"code": "BAD_REQUEST", ...}}
    curl -X POST "$API/things" \
         -d '{"thing_id": "3", "name": "blue"}'   # 201: {"created": "3"}

Every response — success or error — carries the configured CORS headers and the uniform JSON
envelope. Server-side, ``self.stats`` now holds ``api_calls``, per-route counters and
``api_errors_4xx``.


6. Switch on Cognito authorization
----------------------------------

Attach a JWT authorizer to the HTTP API (assuming an existing Cognito User Pool and app client):

..  code-block:: yaml

      ThingsHttpApi:
        Type: AWS::Serverless::HttpApi
        Properties:
          Auth:
            DefaultAuthorizer: CognitoJwt
            Authorizers:
              CognitoJwt:
                JwtConfiguration:
                  issuer: !Sub 'https://cognito-idp.${AWS::Region}.amazonaws.com/${UserPoolId}'
                  audience:
                    - !Ref UserPoolClientId
                IdentitySource: $request.header.Authorization

and flip the Processor config:

..  code-block:: python

    DEFAULT_CONFIG = {
        ...
        'auth_enabled':      True,
        'authorized_groups': ['admins'],    # Optional: require a Cognito group.
    }

The division of labor: **API Gateway verifies the JWT signature** and injects the claims;
``LambdaApi`` enforces their presence and expiry (→ ``401``) and the group membership (→ ``403``),
and exposes them to your handlers as ``self.claims``. Requests without a valid token never reach
your routing — API Gateway rejects unsigned tokens, and ``LambdaApi`` answers ``401`` before
matching any route.

..  code-block:: bash

    curl "$API/things"                                   # 401: no token
    TOKEN=$(aws cognito-idp initiate-auth ... )          # obtain a JWT for a test user
    curl -H "Authorization: Bearer $TOKEN" "$API/things" # 200 (403 if not in `admins`)

For per-route rules (e.g. writes only for admins), override ``check_route_access`` — see
:doc:`the LambdaApi guide <../lambda_api>`.


7. Cleanup
----------

..  code-block:: bash

    sam delete --stack-name things-api
