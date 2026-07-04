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

Internal helper emitting ``DeprecationWarning`` for the legacy orchestration entities of ``sosw``.

Since version 3.0.0 ``sosw`` is a framework for bootstrapping AWS Lambda functions. The orchestration
layer (Orchestrator, Scheduler, Scavenger, Worker, etc.) is deprecated: it stays fully functional
throughout 3.x, warns once per entity per process when instantiated, and will be removed in 4.0.
"""

__all__ = ['warn_deprecated', 'reset_warned_entities']
__author__ = "Nikolay Grishchenko"

import warnings


MIGRATION_GUIDE_URL = 'https://docs.sosw.app/migration_3_0.html'

# Names of entities that have already warned in this process. See `warn_deprecated`.
_WARNED_ENTITIES = set()


def warn_deprecated(entity_name: str, hint: str = ''):
    """
    Emit a ``DeprecationWarning`` for a deprecated ``sosw`` entity, once per `entity_name` per process.

    Designed to be called as the very first statement of the ``__init__`` of a deprecated class.
    The ``stacklevel=3`` then attributes the warning to the line of user code constructing the object:
    user code -> ``SomeDeprecatedClass.__init__`` -> ``warn_deprecated`` -> ``warnings.warn``.

    :param str entity_name: Name of the deprecated entity, e.g. ``'Orchestrator'``.
    :param str hint:        Optional entity-specific migration guidance injected into the message.
    """

    if entity_name in _WARNED_ENTITIES:
        return

    _WARNED_ENTITIES.add(entity_name)

    message = f"{entity_name} is deprecated since sosw 3.0.0 and will be removed in 4.0."
    if hint:
        message += f" {hint}"
    message += f" sosw is now a Lambda bootstrapping framework; see the migration guide: {MIGRATION_GUIDE_URL}"

    warnings.warn(message, DeprecationWarning, stacklevel=3)


def reset_warned_entities():
    """
    Clear the once-per-process guard so that deprecated entities may warn again.
    Intended primarily for tests.
    """

    _WARNED_ENTITIES.clear()
