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

``sosw`` - a framework for bootstrapping AWS Lambda functions.

This package init is a lazy façade (:pep:`562`): ``import sosw`` imports neither ``boto3`` nor any
package modules and emits no warnings. Public names (e.g. ``sosw.Processor``) are resolved and
cached on first attribute access.
"""

__all__ = [
    'Processor',
    'LambdaGlobals',
    'LambdaApi',
    'get_lambda_handler',
]

from importlib import import_module


# Public package attributes are imported lazily from these modules on first access (PEP 562).
_LAZY_ATTRIBUTES = {
    'Processor':          'sosw.app',
    'LambdaGlobals':      'sosw.app',
    'LambdaApi':          'sosw.lambda_api',
    'get_lambda_handler': 'sosw.app',
}

# Names removed in the 3.0 major release together with the whole self-hosted orchestration layer.
# Kept only to raise a helpful AttributeError pointing at the migration options.
_REMOVED_ATTRIBUTES = (
    'Orchestrator',
    'Scavenger',
    'Scheduler',
    'Worker',
    'WorkerAssistant',
    'Labourer',
    'Essential',
)


def _get_version() -> str:
    """
    Version of the installed ``sosw`` distribution, or the release default when running from sources.

    :rtype: str
    """

    from importlib.metadata import PackageNotFoundError, version

    try:
        return version('sosw')
    except PackageNotFoundError:
        return '3.0.0'


def __getattr__(name):
    """
    Resolve public attributes lazily (PEP 562), keeping ``import sosw`` free of heavy imports.

    :param str name:        Name of the requested package attribute.
    :raises AttributeError: If the name is not a public attribute of the package. For the names
                            of the orchestration layer removed in the 3.0 major release the error
                            message explains the migration options.
    """

    if name == '__version__':
        value = _get_version()
    elif name in _LAZY_ATTRIBUTES:
        value = getattr(import_module(_LAZY_ATTRIBUTES[name]), name)
    elif name in _REMOVED_ATTRIBUTES:
        raise AttributeError(
                f"{name} was removed in the 3.0 major release. The orchestration layer is no longer part of "
                f"sosw — pin 'sosw<3' to keep using it, or see https://docs.sosw.app/migration_3_0.html")
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    globals()[name] = value  # Cache, so that the next access does not call `__getattr__` again.
    return value


def __dir__():
    return sorted([*__all__, '__version__'])
