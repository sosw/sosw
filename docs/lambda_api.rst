.. _LambdaApi:

========================================
LambdaApi — Lambdas behind API Gateway
========================================

``sosw.lambda_api.LambdaApi`` is a :ref:`Processor <Processor>` subclass for AWS Lambda functions
that serve HTTP requests behind Amazon API Gateway. One Lambda backs a whole (micro)API: you
declare a route table in the config, implement the handlers as methods, and ``LambdaApi`` takes
care of event parsing, authorization, routing, response rendering and the error contract.

Both API Gateway payload formats are supported: **REST API (v1.0)** and **HTTP API (v2.0)**
proxy events. New in ``sosw`` 3.0.0.


Minimal example
---------------

..  code-block:: python

    from sosw.app import LambdaGlobals, get_lambda_handler
    from sosw.components.exceptions import NotFoundError
    from sosw.lambda_api import LambdaApi


    class Processor(LambdaApi):

        DEFAULT_CONFIG = {
            'auth_enabled': True,
            'routes':       {
                'GET /things':            'list_things',
                'GET /things/{thing_id}': 'get_thing',
                'POST /things':           'create_thing',
            },
            'things_dynamo_db_config': {
                'table_name': 'things',
                'row_mapper': {'thing_id': 'S'},
            },
        }


        def list_things(self, event, **kwargs):
            return {'things': self.get_ddbc('things').get_by_scan()}


        def get_thing(self, event, thing_id=None, **kwargs):
            things = self.get_ddbc('things').get_by_query({'thing_id': thing_id})
            if not things:
                raise NotFoundError(f"Thing {thing_id} does not exist")
            return {'thing': things[0]}


        def create_thing(self, event, **kwargs):
            import json
            thing = json.loads(event.get('body') or '{}')
            self.get_ddbc('things').put(thing)
            return self.make_response({'created': thing['thing_id']}, status_code=201)


    global_vars = LambdaGlobals()
    lambda_handler = get_lambda_handler(Processor, global_vars)

The regular :py:func:`sosw.app.get_lambda_handler` is used — ``LambdaApi`` inherits the full
:ref:`warm start <Warm Start>` behavior, and the compiled router is cached on the instance for
the lifetime of the container.


Routing
-------

The ``routes`` config parameter maps ``'METHOD /path'`` keys to names of handler methods:

* Paths use the API Gateway resource syntax: ``{param}`` matches one path segment, the greedy
  ``{proxy+}`` matches the rest of the path including slashes.
* Handlers receive the raw event as the first positional argument and the extracted path
  parameters (URL-decoded) as keyword arguments: ``def get_thing(self, event, thing_id=None, **kwargs)``.
* Routes without placeholders are matched before parametrized ones, so ``GET /things/count``
  wins over ``GET /things/{thing_id}`` regardless of declaration order.
* Request paths are normalized before matching: the query string and the trailing slash are
  stripped (``/things/`` and ``/things?page=2`` both match ``/things``).
* If the deployed API serves routes under a stage or mount prefix, list it in ``path_prefixes``
  (e.g. ``'path_prefixes': ['/prod']`` turns ``/prod/things`` into ``/things``).

A handler may return:

* any JSON-serializable object — rendered as a ``200`` response;
* a pre-rendered dict containing ``statusCode`` (typically built with ``make_response(body,
  status_code=..., headers=...)``) — passed through to API Gateway untouched;
* or raise an :py:class:`~sosw.components.exceptions.ApiError` subclass — rendered as the
  corresponding HTTP error envelope.


Authorization
-------------

``LambdaApi`` expects the **API Gateway authorizer** (Cognito User Pool authorizer on REST APIs,
JWT authorizer on HTTP APIs) to verify the token signature. The class then works with the claims
the authorizer injected into the event:

* REST API v1: ``requestContext.authorizer.claims``;
* HTTP API v2: ``requestContext.authorizer.jwt.claims``.

With ``auth_enabled: True`` (the secure default) every request must carry claims:

#.  Missing claims → ``401``.
#.  Expired token → ``401``. The ``exp`` claim is tolerated in every shape the authorizers
    deliver: numeric epoch, numeric string (HTTP API v2), and the legacy custom-authorizer string
    format (``'Wed Aug 06 09:02:12 UTC 2025'``). A missing or unparsable ``exp`` is *not* treated
    as expired — the authorizer remains the authority on token validity.
#.  When the ``authorized_groups`` config parameter is non-empty, the caller must belong to at
    least one of the listed Cognito groups (the ``cognito:groups`` claim, tolerated as a real
    list, the stringified ``'[admins users]'`` v2 form, or a comma/space-separated string) →
    otherwise ``403``.

Validated claims are available to handlers as ``self.claims`` (reset on every invocation). With
``auth_enabled: False`` claims are still extracted best-effort into ``self.claims``, but nothing
is enforced.

Error responses never include claim contents — the reasons are logged server-side only.

For per-route rules, override the ``check_route_access(route, claims)`` hook — it runs after
route matching and before the handler:

..  code-block:: python

    class Processor(LambdaApi):

        WRITE_METHODS = ('POST', 'PUT', 'PATCH', 'DELETE')


        def check_route_access(self, route, claims):
            if route.startswith(self.WRITE_METHODS) and 'admins' not in self.parse_groups(
                    claims.get('cognito:groups')):
                raise ForbiddenError("Write access requires the admins group")


Responses, CORS and JSON encoding
---------------------------------

``make_response(body, status_code=200, headers=None)`` renders the API Gateway proxy response
``{'statusCode', 'headers', 'body'}``. Headers are the config-driven ``cors_headers`` (the
defaults allow every origin — tighten them for production) merged with the per-call ``headers``.

The JSON encoder handles the types DynamoDB-backed code returns without ceremony:

* ``Decimal`` → ``int`` when integral, else ``float``;
* ``datetime`` / ``date`` → ISO-8601 string;
* ``set`` / ``frozenset`` → sorted list.

Override the static method ``json_default`` to support more types (e.g. ``UUID``).


The error contract
------------------

Handlers (and the pipeline itself) communicate failures by raising the
:py:class:`~sosw.components.exceptions.ApiError` hierarchy from
:ref:`sosw.components.exceptions <Exceptions>`:

======================== ============ ==================== =========================================
Exception                HTTP status  ``error.code``       Raised by the pipeline when
======================== ============ ==================== =========================================
``BadRequestError``      400          ``BAD_REQUEST``      the event is not an API Gateway proxy event
``UnauthorizedError``    401          ``UNAUTHORIZED``     claims are missing or the token expired
``ForbiddenError``       403          ``FORBIDDEN``        the caller is in none of ``authorized_groups``
``NotFoundError``        404          ``NOT_FOUND``        no configured route matches method + path
``ConflictError``        409          ``CONFLICT``         (your handlers)
``ServerError``          500          ``SERVER_ERROR``     any unexpected exception
======================== ============ ==================== =========================================

Every error renders the same machine-readable envelope:

..  code-block:: json

    {
        "error": {
            "code":    "NOT_FOUND",
            "message": "No route configured for: GET /nope"
        }
    }

A custom message travels in ``message``; ``raise ConflictError(f"Thing {thing_id} already
exists")`` produces HTTP 409 with the envelope carrying your text.

The exact semantics of the pipeline:

* **Authorization runs before routing.** A request with missing/invalid claims receives ``401``
  even for a path that does not exist — unauthenticated callers cannot probe the route table.
* **Unknown route or method → 404.** There is no automatic ``405``: a known path with an
  unconfigured method is just as much "not found".
* **Only ApiError maps to specific statuses.** Any other exception escaping a handler is logged
  server-side with the full traceback, counted in stats, and rendered as the *generic* ``500``
  envelope — internals never leak to the client.

..  note::

    Unlike regular ``sosw`` Processors, which must fail fast and loud, ``LambdaApi.__call__``
    intentionally converts exceptions into well-formed HTTP responses: an API Gateway integration
    must always answer the client. This is the API contract layer, not silent error handling —
    everything is logged and counted server-side.


REST API (v1) vs HTTP API (v2)
------------------------------

``LambdaApi`` absorbs the differences between the two payload formats; for reference:

========================== ========================================= =========================================
Aspect                     REST API (payload v1.0)                   HTTP API (payload v2.0)
========================== ========================================= =========================================
Method / path              ``httpMethod`` + ``path``                 ``requestContext.http.method`` + ``rawPath``
Claims                     ``requestContext.authorizer.claims``      ``requestContext.authorizer.jwt.claims``
``exp`` claim type         numeric                                   numeric **string**
``cognito:groups`` claim   list                                      stringified ``'[admins users]'``
Fallback                   —                                         ``routeKey`` (``'GET /things'``) for trimmed events
========================== ========================================= =========================================


Statistics
----------

``LambdaApi`` counts per-container statistics through the standard ``self.stats`` mechanism:

* ``api_calls`` — every invocation;
* ``api_route_get_things_thing_id`` — per matched route
  (snake_case of the route key, e.g. ``'GET /things/{thing_id}'``);
* ``api_errors_4xx`` / ``api_errors_5xx`` — per error family.


Deployment
----------

See the :doc:`HTTP API with LambdaApi <tutorials/http_api>` tutorial for a complete SAM template
with a Cognito JWT authorizer and ``curl``-level verification of the whole contract.


API reference
-------------

..  automodule:: sosw.lambda_api
    :members:
