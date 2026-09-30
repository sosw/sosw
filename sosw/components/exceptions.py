class EventNotFromSourceException(ValueError):
    pass


class ApiError(Exception):
    """
    Base class for exceptions that map to an HTTP response of :py:class:`sosw.lambda_api.LambdaApi`.

    Handlers raise these (or subclasses); ``LambdaApi`` renders them as a JSON error envelope:
    ``{'error': {'code': error_code, 'message': message}}`` with the appropriate HTTP status code.

    Subclasses override the class attributes; per-instance overrides are supported through
    the constructor keyword arguments.

    :param str message:     Human readable error description. Defaults to ``default_message``.
    :param int status_code: Optional per-instance override of the HTTP status code.
    :param str error_code:  Optional per-instance override of the machine-readable error code.
    """

    status_code = 500
    error_code = 'SERVER_ERROR'
    default_message = 'Internal server error'


    def __init__(self, message: str = None, status_code: int = None, error_code: str = None):
        self.message = message if message is not None else self.default_message

        if status_code is not None:
            self.status_code = status_code

        if error_code is not None:
            self.error_code = error_code

        super().__init__(self.message)


class BadRequestError(ApiError):
    """The request is malformed or fails validation. Maps to HTTP 400."""

    status_code = 400
    error_code = 'BAD_REQUEST'
    default_message = 'Bad request'


class UnauthorizedError(ApiError):
    """The caller identity cannot be established (missing/expired credentials). Maps to HTTP 401."""

    status_code = 401
    error_code = 'UNAUTHORIZED'
    default_message = 'Unauthorized'


class ForbiddenError(ApiError):
    """The caller is authenticated, but not allowed to perform the action. Maps to HTTP 403."""

    status_code = 403
    error_code = 'FORBIDDEN'
    default_message = 'Forbidden'


class NotFoundError(ApiError):
    """The requested resource or route does not exist. Maps to HTTP 404."""

    status_code = 404
    error_code = 'NOT_FOUND'
    default_message = 'Not found'


class ConflictError(ApiError):
    """The request conflicts with the current state of the resource. Maps to HTTP 409."""

    status_code = 409
    error_code = 'CONFLICT'
    default_message = 'Conflict'


class ServerError(ApiError):
    """Unexpected server-side failure. Maps to HTTP 500."""

    status_code = 500
    error_code = 'SERVER_ERROR'
    default_message = 'Internal server error'
