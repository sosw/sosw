"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - a framework for bootstrapping AWS Lambda functions

    The MIT License (MIT)
    Copyright (C) 2026  sosw core contributors <info@sosw.app>

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

__all__ = ['LambdaApi']
__author__ = "Nikolay Grishchenko"

try:
    from aws_lambda_powertools import Logger

    logger = Logger(child=True)

except ImportError:
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

import json
import re
import time

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import unquote

from sosw.app import Processor
from sosw.components.exceptions import ApiError, BadRequestError, ForbiddenError, NotFoundError, ServerError, \
    UnauthorizedError


class LambdaApi(Processor):
    """
    Base Processor for AWS Lambda functions behind API Gateway. Supports both the REST API (payload
    version 1.0) and the HTTP API (payload version 2.0) proxy event shapes.

    Provides a declarative router configured with the ``routes`` config parameter, Cognito JWT claims
    extraction and validation, config-driven CORS headers, JSON response rendering with a
    DynamoDB-friendly encoder, and a uniform machine-readable error envelope.

    Subclasses implement the handler methods and declare them in ``routes``:

    ..  code-block:: python

        from sosw.app import LambdaGlobals, get_lambda_handler
        from sosw.lambda_api import LambdaApi

        class Processor(LambdaApi):

            DEFAULT_CONFIG = {
                'auth_enabled': True,
                'routes':       {
                    'GET /things':            'list_things',
                    'GET /things/{thing_id}': 'get_thing',
                },
            }

            def list_things(self, event, **kwargs):
                return {'things': self.get_ddbc('things').scan()}

            def get_thing(self, event, thing_id=None, **kwargs):
                return {'thing': self.get_ddbc('things').get_by_query({'thing_id': thing_id})}

        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(Processor, global_vars)

    Handlers receive the raw event as the first positional argument and the path parameters extracted
    from the request path (per the ``{param}`` placeholders of the matched route) as keyword arguments.
    A handler may return:

    * any JSON-serializable object - rendered as a ``200`` response body;
    * a pre-rendered response dict containing ``statusCode`` (e.g. built with :meth:`make_response`
      using a custom status code or extra headers) - passed through to API Gateway untouched;
    * or raise :py:class:`sosw.components.exceptions.ApiError` (or a subclass) - rendered as the
      corresponding HTTP error envelope.

    ..  note::

        Unlike regular ``sosw`` Processors which are required to fail fast and loud, ``__call__``
        of this class intentionally converts exceptions to well-formed HTTP error responses:
        an API Gateway integration must always return a valid response to the client. This is the
        API contract layer, not silent error handling: every exception is logged server-side
        (unexpected ones with the full traceback), counted in ``self.stats``, and unexpected
        exceptions are never exposed to the client beyond a generic ``500`` envelope.

    Configuration parameters of this class:

    * ``routes`` - dict mapping ``'METHOD /path'`` route keys to names of handler methods.
      Paths support API Gateway resource syntax placeholders: ``{param}`` and greedy ``{proxy+}``.
    * ``auth_enabled`` - when ``True`` (secure default), requests must carry Cognito claims
      injected by the API Gateway authorizer, otherwise ``401`` is returned.
    * ``authorized_groups`` - optional list of Cognito group names. When non-empty, the caller
      must be a member of at least one of them (the ``cognito:groups`` claim), otherwise ``403``.
    * ``cors_headers`` - dict of headers attached to every rendered response.
    * ``path_prefixes`` - optional list of prefixes (e.g. stage or mount prefixes) stripped from
      the request path before route matching.
    """

    DEFAULT_CONFIG = {
        'auth_enabled':      True,
        'authorized_groups': [],
        'routes':            {},
        'path_prefixes':     [],
        'cors_headers':      {
            'Access-Control-Allow-Origin':  '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept, Origin',
            'Content-Type':                 'application/json',
        },
    }

    # Claims of the caller of the request currently being processed. Reset at the beginning of every
    # invocation, populated after successful authorization (or best-effort when `auth_enabled` is off).
    claims: dict = None


    def __init__(self, *args, **kwargs):
        """
        Initialize the Processor and the container-lifetime router cache.

        The compiled router is intentionally an instance-level cache: warm Lambda containers reuse
        the Processor instance across invocations (see :py:class:`sosw.app.LambdaGlobals`), so the
        routes are parsed and their patterns compiled only once per container.
        """

        super().__init__(*args, **kwargs)

        self._router = None
        self.claims = {}


    def __call__(self, event: dict, *args, **kwargs) -> dict:
        """
        Dispatch an API Gateway proxy event to the configured route handler and render the response.

        The processing pipeline: parse the event (v1/v2) -> authorize the caller -> match the route ->
        run the :meth:`check_route_access` hook -> call the handler -> render the result.

        Any :py:class:`sosw.components.exceptions.ApiError` raised during the pipeline (including
        from handlers) is rendered as its HTTP error envelope. Any unexpected exception is logged
        with the full traceback and rendered as a generic ``500`` envelope without leaking internals.

        :param dict event:  API Gateway proxy event (REST v1 or HTTP v2).
        :rtype:             dict
        :return:            API Gateway proxy response: ``{'statusCode', 'headers', 'body'}``.
        """

        super().__call__(event, *args, **kwargs)

        self.stats['api_calls'] += 1
        self.claims = {}

        try:
            method, path = self.parse_event(event)

            if self._c('auth_enabled', True):
                self.claims = self.validate_authorization(event)
            else:
                self.claims = self.get_claims(event)

            route, path_params = self.match_route(method, path)
            self.stats[self.route_stats_key(route['route'])] += 1

            self.check_route_access(route['route'], self.claims)

            result = route['handler'](event, **path_params)

            if isinstance(result, dict) and 'statusCode' in result:
                return result

            return self.make_response(result)

        except ApiError as err:
            logger.warning("API request failed with %s (HTTP %s): %s",
                           err.__class__.__name__, err.status_code, err)
            return self.make_error_response(err)

        except Exception:
            logger.exception("Unexpected exception while processing the API request")
            return self.make_error_response(ServerError())


    def parse_event(self, event: dict) -> tuple:
        """
        Extract the HTTP method and the normalized request path from an API Gateway proxy event.

        Supported event shapes:

        * REST API v1: ``httpMethod`` + ``path``;
        * HTTP API v2: ``requestContext.http.method`` + ``rawPath``;
        * trimmed v2 events carrying only ``routeKey`` (``'GET /things'``) as the last resort.

        The path is normalized (query string and trailing slash removed) and the first matching
        entry of the ``path_prefixes`` config parameter is stripped from it.

        :param dict event:          Raw Lambda event.
        :rtype:                     tuple
        :return:                    Tuple of (method, path), method upper-cased.
        :raises BadRequestError:    If the event does not look like an API Gateway proxy event.
        """

        method = event.get('httpMethod')
        path = event.get('path')

        if not method:
            http = (event.get('requestContext') or {}).get('http') or {}
            method = http.get('method')
            path = event.get('rawPath') or http.get('path')

        if not method and ' ' in str(event.get('routeKey') or ''):
            method, _, path = event['routeKey'].partition(' ')

        if not method or not path:
            raise BadRequestError("Unable to determine HTTP method and path from the event")

        return method.upper(), self.strip_path_prefix(self._normalize_path(path))


    def get_router(self) -> list:
        """
        Return the compiled router, building it on the first request of the container.

        The router is built from the ``routes`` config parameter by :meth:`build_router` and cached
        on the instance for the container lifetime, so warm invocations skip parsing and regex
        compilation entirely.

        :rtype:     list
        :return:    List of compiled route entries.
        """

        if self._router is None:
            self._router = self.build_router()

        return self._router


    def build_router(self) -> list:
        """
        Compile the ``routes`` config parameter into a list of route entries.

        Each ``'METHOD /path': 'handler_name'`` config pair becomes a dict with the original route
        key, the upper-cased method, the normalized path template, the compiled path pattern and
        the bound handler method. Routes without path parameters are ordered before parametrized
        ones, so ``GET /things/count`` wins over ``GET /things/{thing_id}`` regardless of the
        declaration order. Within each group the config declaration order is preserved.

        :rtype:             list
        :return:            List of compiled route entries.
        :raises ValueError: If a route key is malformed or a handler method is missing.
        """

        router = []

        for route_key, handler_name in (self._c('routes') or {}).items():
            method, _, path_template = str(route_key).strip().partition(' ')
            path_template = path_template.strip()

            if not method or not path_template:
                raise ValueError(f"Invalid route {route_key!r}. Expected format: 'METHOD /path'")

            handler = getattr(self, handler_name, None) if isinstance(handler_name, str) else None
            if not callable(handler):
                raise ValueError(f"Route {route_key!r} points to {handler_name!r} which is not "
                                 f"a method of {self.__class__.__name__}")

            path_template = self._normalize_path(path_template)

            router.append({
                'route':   str(route_key),
                'method':  method.upper(),
                'path':    path_template,
                'pattern': self.compile_path_pattern(path_template),
                'handler': handler,
            })

        router.sort(key=lambda entry: '{' in entry['path'])

        return router


    @staticmethod
    def compile_path_pattern(path_template: str) -> re.Pattern:
        """
        Compile a path template with API Gateway resource syntax into a regex pattern.

        ``{param}`` placeholders match a single path segment as the named group ``param``.
        The greedy ``{proxy+}`` form matches the rest of the path (including slashes) as ``proxy``.

        :param str path_template:   Path template, e.g. ``'/things/{thing_id}'``.
        :rtype:                     re.Pattern
        :return:                    Compiled pattern matching the whole request path.
        """

        pattern = ''

        for token in re.split(r'({[^{}]+})', path_template):
            if token.startswith('{') and token.endswith('}'):
                name = token[1:-1]
                if name.endswith('+'):
                    pattern += f'(?P<{name[:-1]}>.+)'
                else:
                    pattern += f'(?P<{name}>[^/]+)'
            else:
                pattern += re.escape(token)

        return re.compile(f'^{pattern}$')


    def match_route(self, method: str, path: str) -> tuple:
        """
        Find the route entry matching the request and extract its path parameters.

        Path parameter values are URL-decoded. Requests that match no configured route (either by
        path or by method) raise :py:class:`sosw.components.exceptions.NotFoundError` -> ``404``.

        :param str method:      Upper-cased HTTP method of the request.
        :param str path:        Normalized request path.
        :rtype:                 tuple
        :return:                Tuple of (route entry, dict of extracted path parameters).
        :raises NotFoundError:  When no configured route matches the request.
        """

        for route in self.get_router():
            if route['method'] != method:
                continue

            match = route['pattern'].match(path)
            if match:
                return route, {name: unquote(value) for name, value in match.groupdict().items()}

        raise NotFoundError(f"No route configured for: {method} {path}")


    @staticmethod
    def get_claims(event: dict) -> dict:
        """
        Extract Cognito JWT claims from either API Gateway authorizer event shape.

        * REST API v1 (Cognito User Pool authorizer): ``requestContext.authorizer.claims``
        * HTTP API v2 (JWT authorizer): ``requestContext.authorizer.jwt.claims``

        Both the id and access token claim sets are supported - the method does not require any
        specific claim to be present.

        :param dict event:  API Gateway proxy event.
        :rtype:             dict
        :return:            The claims dictionary, or an empty dict if the event carries none.
        """

        authorizer = (event.get('requestContext') or {}).get('authorizer')
        if not isinstance(authorizer, dict):
            authorizer = {}

        claims = authorizer.get('claims')
        if not isinstance(claims, dict):
            jwt_data = authorizer.get('jwt')
            claims = jwt_data.get('claims') if isinstance(jwt_data, dict) else None

        return claims if isinstance(claims, dict) else {}


    def validate_authorization(self, event: dict) -> dict:
        """
        Validate the caller identity from the claims injected by the API Gateway authorizer.

        The authorizer has already verified the JWT signature; this method enforces that claims are
        present, the token is not expired, and (when the ``authorized_groups`` config parameter is
        non-empty) the caller belongs to at least one authorized Cognito group.

        Error responses never include claim contents; the reasons are logged server-side only.

        :param dict event:              API Gateway proxy event.
        :rtype:                         dict
        :return:                        The validated claims.
        :raises UnauthorizedError:      When claims are missing or the token is expired -> ``401``.
        :raises ForbiddenError:         When the caller is not in any authorized group -> ``403``.
        """

        claims = self.get_claims(event)

        if not claims:
            logger.warning("No authorization claims found in the event")
            raise UnauthorizedError()

        if self.token_expired(claims.get('exp')):
            logger.warning("Authorization token of the caller has expired")
            raise UnauthorizedError()

        authorized_groups = self._c('authorized_groups') or []
        if authorized_groups:
            caller_groups = self.parse_groups(claims.get('cognito:groups'))
            if not set(map(str, authorized_groups)) & set(caller_groups):
                logger.warning("Caller is not a member of any of the authorized Cognito groups")
                raise ForbiddenError()

        return claims


    @staticmethod
    def token_expired(exp: Any) -> bool:
        """
        Check whether the ``exp`` claim points to the past.

        Tolerated formats: numeric epoch (the standard JWT claim), numeric strings (the form
        HTTP API v2 authorizers deliver), and the legacy custom-authorizer string format
        ``'Wed Aug 06 09:02:12 UTC 2025'``. A missing or unparsable claim is not treated as
        expired - the API Gateway authorizer remains the authority on token validity.

        :param exp: Value of the ``exp`` claim in any supported format.
        :rtype:     bool
        """

        if exp is None:
            return False

        if isinstance(exp, (int, float, Decimal)):
            return float(exp) < time.time()

        if isinstance(exp, str):
            value = exp.strip()

            if re.fullmatch(r'\d+(\.\d+)?', value):
                return float(value) < time.time()

            try:
                parsed = datetime.strptime(value, '%a %b %d %H:%M:%S UTC %Y')
            except ValueError:
                logger.warning("Unsupported string format of the 'exp' claim. Skipping the expiration check.")
                return False

            return parsed.replace(tzinfo=timezone.utc).timestamp() < time.time()

        logger.warning("Unsupported type of the 'exp' claim: %s. Skipping the expiration check.",
                       type(exp).__name__)
        return False


    @staticmethod
    def parse_groups(raw_groups: Any) -> list:
        """
        Parse the ``cognito:groups`` claim into a list of group names.

        Tolerated formats: a real list (REST API v1), the stringified ``'[admins users]'`` form
        delivered by HTTP API v2 JWT authorizers, and comma- or space-separated plain strings.

        :param raw_groups:  Raw value of the ``cognito:groups`` claim.
        :rtype:             list
        """

        if not raw_groups:
            return []

        if isinstance(raw_groups, (list, tuple, set)):
            return [str(group).strip() for group in raw_groups if str(group).strip()]

        if isinstance(raw_groups, str):
            return [group for group in re.split(r'[,\s]+', raw_groups.strip('[] ')) if group]

        return [str(raw_groups)]


    def check_route_access(self, route: str, claims: dict) -> None:
        """
        Per-route access control hook, called after route matching and before the handler.

        The base implementation allows everything. Override in subclasses to implement custom RBAC
        (e.g. permission tables in DynamoDB, interface grants, organization scopes) and raise
        :py:class:`sosw.components.exceptions.ForbiddenError` (-> ``403``) or
        :py:class:`sosw.components.exceptions.UnauthorizedError` (-> ``401``) to deny the request.

        :param str route:   The matched route key exactly as declared in the ``routes`` config,
                            e.g. ``'GET /things/{thing_id}'``.
        :param dict claims: Validated claims of the caller (empty dict when ``auth_enabled`` is off
                            and the event carries no claims).
        """

        logger.debug("Default check_route_access() allows route: %s", route)


    def make_response(self, body: Any = None, status_code: int = 200, headers: dict = None) -> dict:
        """
        Render an API Gateway proxy response with the configured CORS headers.

        The body is JSON-serialized with :meth:`json_default`, which supports the types DynamoDB
        and datetime-heavy code commonly return (``Decimal``, ``datetime``, ``date``, ``set``).

        :param body:            JSON-serializable payload, or ``None`` for an empty body.
        :param int status_code: HTTP status code of the response.
        :param dict headers:    Extra headers merged over the configured ``cors_headers``.
        :rtype:                 dict
        :return:                API Gateway proxy response: ``{'statusCode', 'headers', 'body'}``.
        """

        response_headers = dict(self._c('cors_headers') or {})
        response_headers.update(headers or {})

        return {
            'statusCode': status_code,
            'headers':    response_headers,
            'body':       '' if body is None else json.dumps(body, default=self.json_default),
        }


    def make_error_response(self, error: ApiError) -> dict:
        """
        Render the uniform error envelope for an ApiError and count the error statistics.

        The envelope is ``{'error': {'code': <MACHINE_CODE>, 'message': <human readable>}}``.
        Statistics: ``api_errors_4xx`` / ``api_errors_5xx`` per status code family.

        :param ApiError error:  The error to render.
        :rtype:                 dict
        :return:                API Gateway proxy response with the error envelope.
        """

        self.stats[f'api_errors_{error.status_code // 100}xx'] += 1

        body = {
            'error': {
                'code':    error.error_code,
                'message': str(error),
            },
        }

        return self.make_response(body, status_code=error.status_code)


    @staticmethod
    def json_default(value: Any) -> Any:
        """
        JSON encoder fallback for types that ``json.dumps`` does not handle natively.

        * ``Decimal`` (as returned by DynamoDB) - ``int`` when integral, else ``float``;
        * ``datetime`` / ``date`` - ISO-8601 string;
        * ``set`` / ``frozenset`` - sorted list (plain list when elements are not sortable).

        Override in subclasses to support more types (e.g. ``UUID``).

        :param value:       Value that JSON could not serialize.
        :return:            A JSON-serializable representation.
        :raises TypeError:  For unsupported types (standard ``json`` contract).
        """

        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, (set, frozenset)):
            try:
                return sorted(value)
            except TypeError:
                return list(value)

        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


    def strip_path_prefix(self, path: str) -> str:
        """
        Strip the first matching entry of the ``path_prefixes`` config parameter from the path.

        Useful when the deployed API serves routes under a stage or mount prefix
        (e.g. ``/prod/things`` -> ``/things`` with ``'path_prefixes': ['/prod']``).

        :param str path:    Normalized request path.
        :rtype:             str
        """

        for prefix in self._c('path_prefixes') or []:
            prefix = self._normalize_path(prefix)

            if prefix != '/' and (path == prefix or path.startswith(prefix + '/')):
                stripped = path[len(prefix):]
                return stripped if stripped.startswith('/') else f'/{stripped}'

        return path


    @staticmethod
    def _normalize_path(path: str) -> str:
        """
        Normalize a path: strip the query string and the trailing slash, ensure a leading slash.

        :param str path:    Raw path.
        :rtype:             str
        """

        path = str(path or '').split('?', 1)[0].strip()

        if not path.startswith('/'):
            path = f'/{path}'

        if len(path) > 1:
            path = path.rstrip('/') or '/'

        return path


    @staticmethod
    def route_stats_key(route: str) -> str:
        """
        Build the snake_case ``self.stats`` counter name for a route key.

        E.g. ``'GET /things/{thing_id}'`` -> ``'api_route_get_things_thing_id'``.

        :param str route:   Route key as declared in the ``routes`` config.
        :rtype:             str
        """

        return 'api_route_' + re.sub(r'[^a-zA-Z0-9]+', '_', str(route)).strip('_').lower()
