import importlib
import logging
import os
import sys
import unittest

from unittest.mock import MagicMock, patch

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

import sosw.app
import sosw.essential
import sosw.labourer
import sosw.orchestrator
import sosw.scavenger
import sosw.scheduler
import sosw.worker
import sosw.components.config
import sosw.components.dynamo_db
import sosw.components.siblings
import sosw.components.sns
import sosw.managers.ecology
import sosw.managers.meta_handler
import sosw.managers.task


class powertools_guards_UnitTestCase(unittest.TestCase):
    """
    Covers the optional ``aws_lambda_powertools`` import guard of every sosw module that has one.

    Each module is reloaded with a fake ``aws_lambda_powertools`` package injected into ``sys.modules``
    to execute the success branch of the guard. Afterwards the pre-test module state is restored from
    a snapshot of the module ``__dict__``: unlike a second reload, this puts back the very same class
    and function objects, so references captured by other test modules at import time (``isinstance``
    checks, ``assertRaises`` on exception classes) stay consistent for the rest of the suite.
    The ImportError branch itself is covered by the initial import of the suite: powertools is not
    installed in the test environment.

    Only the import-time wiring is asserted here. No sosw code paths are executed while the fake
    package is in place: some modules (e.g. ``sosw.managers.meta_handler``) reference names that only
    exist when the real ImportError branch of the guard was taken.

    ``sosw.lambda_api`` is intentionally not listed: its powertools branch is covered by its own test
    in ``sosw/test/unit/test_lambda_api.py``.
    """

    # Guarded module -> kwargs the module is expected to construct its powertools Logger with.
    GUARDED_MODULES = [
        (sosw.app, {}),
        (sosw.essential, {}),
        (sosw.labourer, {}),
        (sosw.orchestrator, {}),
        (sosw.scavenger, {}),
        (sosw.scheduler, {}),
        (sosw.worker, {}),
        (sosw.components.config, {'child': True}),
        (sosw.components.dynamo_db, {'child': True}),
        (sosw.components.siblings, {}),
        (sosw.components.sns, {}),
        (sosw.managers.ecology, {}),
        (sosw.managers.meta_handler, {}),
        (sosw.managers.task, {}),
    ]


    def test_powertools_available__module_wires_powertools_logger(self):
        for module, expected_kwargs in self.GUARDED_MODULES:
            with self.subTest(module=module.__name__):
                fake_powertools = MagicMock()
                snapshot = dict(module.__dict__)

                try:
                    with patch.dict(sys.modules, {'aws_lambda_powertools': fake_powertools}):
                        reloaded = importlib.reload(module)

                        self.assertIs(reloaded, module)
                        fake_powertools.Logger.assert_called_once_with(**expected_kwargs)
                        self.assertIs(reloaded.logger, fake_powertools.Logger.return_value)

                finally:
                    # Restore the exact pre-test objects of the module. The module `__dict__` object
                    # itself must stay the same: it is the `__globals__` of every function defined in
                    # the module.
                    module.__dict__.clear()
                    module.__dict__.update(snapshot)

                self.assertIs(module.logger, logging.getLogger())


if __name__ == '__main__':
    unittest.main()
