"""
Unit tests for the lazy package façade in ``sosw/__init__.py`` (PEP 562).

Covers the lazy attribute resolution, the ``__version__`` machinery, and the helpful
``AttributeError`` raised for the orchestration entities removed in the 3.0 major release.
"""

import os
import subprocess
import sys
import unittest
import warnings

from importlib import import_module
from pathlib import Path
from unittest.mock import patch


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

import sosw
import sosw.app


class sosw_init_UnitTestCase(unittest.TestCase):

    REMOVED_NAMES = ['Orchestrator', 'Scavenger', 'Scheduler', 'Worker', 'WorkerAssistant', 'Labourer', 'Essential']


    def test_import_sosw__lazy_and_warning_free(self):
        """
        ``import sosw`` must emit no warnings and must import neither ``boto3`` nor any package
        submodule. Verified in a subprocess to get a clean interpreter without ``sosw`` pre-imported.
        """

        code = ("import sys, warnings; warnings.simplefilter('error'); import sosw; "
                "assert 'sosw.app' not in sys.modules, 'sosw.app imported eagerly'; "
                "assert 'boto3' not in sys.modules, 'boto3 imported eagerly'")

        result = subprocess.run([sys.executable, '-c', code], cwd=Path(__file__).resolve().parents[3],
                                capture_output=True, text=True, timeout=120)

        self.assertEqual(0, result.returncode, f"Subprocess failed: {result.stderr}")


    def test_facade__resolves_public_names(self):
        for name, module_path in sosw._LAZY_ATTRIBUTES.items():
            with self.subTest(name=name):
                sosw.__dict__.pop(name, None)  # Drop the cache so that `__getattr__` runs again.

                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    resolved = getattr(sosw, name)

                # Compare with the current module attribute: other tests of the suite may have
                # reloaded the module, so an import captured at the top of this file could be stale.
                self.assertIs(getattr(import_module(module_path), name), resolved)
                self.assertEqual([], [str(w.message) for w in caught], f"Resolving sosw.{name} must not warn")


    def test_facade__caches_resolved_names(self):
        sosw.__dict__.pop('Processor', None)

        self.assertIs(sosw.app.Processor, sosw.Processor)
        self.assertIn('Processor', sosw.__dict__, "Resolved attribute must be cached in the module globals")


    def test_facade__removed_orchestration_names_raise_with_guidance(self):
        for name in self.REMOVED_NAMES:
            with self.subTest(name=name):
                with self.assertRaises(AttributeError) as cm:
                    getattr(sosw, name)

                message = str(cm.exception)
                self.assertIn(f"{name} was removed in the 3.0 major release", message)
                self.assertIn("The orchestration layer is no longer part of sosw", message)
                self.assertIn("pin 'sosw<3' to keep using it", message)
                self.assertIn('https://docs.sosw.app/migration_3_0.html', message)


    def test_facade__removed_names_not_in_dir_or_all(self):
        for name in self.REMOVED_NAMES:
            self.assertNotIn(name, sosw.__all__)
            self.assertNotIn(name, dir(sosw))


    def test_facade__unknown_attribute_raises(self):
        self.assertRaises(AttributeError, getattr, sosw, 'NoSuchAttribute')


    def test_facade__dir_contains_public_names(self):
        names = dir(sosw)

        for name in ['Processor', 'LambdaGlobals', 'LambdaApi', 'get_lambda_handler', '__version__']:
            self.assertIn(name, names)


    def test_version__is_string(self):
        sosw.__dict__.pop('__version__', None)  # Drop the cache so that `__getattr__` runs again.

        self.assertIsInstance(sosw.__version__, str)
        self.assertTrue(len(sosw.__version__) > 0)


    def test_get_version__installed_distribution(self):
        with patch('importlib.metadata.version', return_value='3.9.9') as mock_version:
            self.assertEqual('3.9.9', sosw._get_version())

        mock_version.assert_called_once_with('sosw')


    def test_get_version__fallback_when_not_installed(self):
        from importlib.metadata import PackageNotFoundError

        with patch('importlib.metadata.version', side_effect=PackageNotFoundError):
            self.assertEqual('3.0.0', sosw._get_version())


if __name__ == '__main__':
    unittest.main()
