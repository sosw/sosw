.. _Exceptions:

Exceptions
----------

Exception classes shared across the framework. The :py:class:`~sosw.components.exceptions.ApiError`
hierarchy is the error contract of :doc:`LambdaApi <../lambda_api>`: handlers raise
these, and they render as JSON error envelopes with the matching HTTP status code.

..  automodule:: sosw.components.exceptions
    :members:
