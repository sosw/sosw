"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2025  sosw core contributors <info@sosw.app>

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

__all__ = ['get_durable_lambda_handler', 'durable_wait', 'parse_durable_result',
           'DURABLE_SDK_AVAILABLE', 'durable_step', 'Duration']
__author__ = "Nikolay Grishchenko"

import json
import logging
import time

import sosw.app

from sosw.app import _make_lambda_handler

# Optional AWS Lambda Durable Execution support. The SDK is an optional dependency of sosw:
# install with `pip install sosw[durable]`. The managed AWS Lambda runtimes also bundle the SDK,
# but AWS recommends pinning it in production deployment packages. The module itself must import
# cleanly without the SDK so that the SDK-optional helpers (`durable_wait`, `parse_durable_result`)
# stay usable in regular Lambdas. Only `get_durable_lambda_handler` genuinely requires the SDK
# and it fails loudly if the SDK is missing.
try:
    from aws_durable_execution_sdk_python import durable_execution, durable_step
    from aws_durable_execution_sdk_python.config import Duration

    DURABLE_SDK_AVAILABLE = True

except ImportError:
    durable_execution = durable_step = Duration = None

    DURABLE_SDK_AVAILABLE = False


logger = logging.getLogger()


def get_durable_lambda_handler(processor_class, global_vars=None, custom_config=None, **durable_kwargs):
    """
    Durable twin of :func:`sosw.app.get_lambda_handler`.

    Builds exactly the same warm-start caching ``lambda_handler`` (single Processor instance cached in
    ``global_vars`` for the lifetime of the Lambda container) and wraps it with the ``durable_execution``
    decorator of the AWS Lambda Durable Execution SDK. Inside the Processor, ``global_vars.lambda_context``
    is the ``DurableContext`` of the current invocation: use its ``step()`` / ``wait()`` operations for
    checkpointed durable operations.

    Requires the optional durable SDK: ``pip install sosw[durable]``. Regular functions should keep
    using :func:`sosw.app.get_lambda_handler`, which does not even import the SDK.

    :param processor_class:  Callable processor class.
    :param global_vars:      Lambda's global variables (processor, context).
    :param custom_config:    Custom configuration to pass the processor constructor.
    :param durable_kwargs:   Optional keyword arguments to pass through to the ``durable_execution``
                             decorator of the SDK.
    :return: Function reference for the lambda handler.
    """

    if not DURABLE_SDK_AVAILABLE:
        raise ImportError(
                "aws-durable-execution-sdk-python is not installed. Run `pip install sosw[durable]` "
                "(or add aws-durable-execution-sdk-python to your Lambda requirements). "
                "Non-durable functions should keep using sosw.app.get_lambda_handler.")

    handler = _make_lambda_handler(processor_class, global_vars, custom_config)

    return durable_execution(handler, **durable_kwargs)


def durable_wait(seconds: int, global_vars=None):
    """
    Wait either using the checkpointed ``wait()`` of the Durable Execution context (when running
    as a durable function), or falling back to a regular ``time.sleep()`` otherwise.

    This helper lets the very same Processor code run both as a durable and as a regular Lambda.
    The fallback path does not require the durable SDK to be installed.

    :param int seconds:     Number of seconds to wait.
    :param global_vars:     Optional instance of ``LambdaGlobals`` holding the ``lambda_context``.
                            Defaults to the module-level ``sosw.app.global_vars``.
    """

    gv = global_vars if global_vars is not None else sosw.app.global_vars
    ctx = getattr(gv, 'lambda_context', None)

    if ctx is not None and hasattr(ctx, 'wait') and Duration is not None:
        ctx.wait(Duration(seconds=seconds))
    else:
        logger.debug("No DurableContext available, falling back to time.sleep(%s)", seconds)
        time.sleep(seconds)


def parse_durable_result(payload):
    """
    Unwrap the invocation envelope returned by a synchronous boto3 ``invoke()`` of a durable function:
    ``{"Status": "SUCCEEDED" | "FAILED" | "PENDING", "Result": <JSON string>, ...}``.

    The business output of the durable handler is JSON-encoded inside ``Result`` when the ``Status``
    is ``SUCCEEDED``. Payloads that are not durable envelopes are returned unchanged, so the callers
    can treat durable and regular Lambdas uniformly. This helper is pure Python for the CALLER side:
    the durable SDK is not required.

    :param payload:         Parsed JSON payload of the Lambda invocation response.
    :return:                The unwrapped result of the durable execution (or the original payload).
    :raises RuntimeError:   If the durable execution FAILED, is still PENDING, or the ``Result``
                            is empty or not valid JSON.
    """

    if not isinstance(payload, dict):
        return payload

    status = payload.get('Status')
    if status not in ('SUCCEEDED', 'FAILED', 'PENDING'):
        return payload

    if status == 'FAILED':
        error = payload.get('Error')
        raise RuntimeError(f"Durable execution failed: {json.dumps(error) if error is not None else 'unknown'}")

    if status == 'PENDING':
        raise RuntimeError("Durable execution returned Status=PENDING: "
                           "the synchronous invoke did not receive a final result.")

    # Status == 'SUCCEEDED'
    result = payload.get('Result')
    if result is None or (isinstance(result, str) and not result.strip()):
        raise RuntimeError("Durable execution SUCCEEDED with an empty Result. Results over the checkpoint "
                           "size limit are not returned via Invoke. Check the logs of the durable function.")

    if isinstance(result, dict):
        return result

    if isinstance(result, str):
        try:
            return json.loads(result)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Durable execution Result is not valid JSON: {e}") from e

    raise RuntimeError(f"Unexpected type of durable execution Result: {type(result).__name__}")
