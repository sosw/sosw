# Core applications
from .unit.test_app import app_UnitTestCase
from .unit.test_labourer import Labourer_UnitTestCase
from .unit.test_orchestrator import Orchestrator_UnitTestCase
from .unit.test_scavenger import Scavenger_UnitTestCase
from .unit.test_scheduler import Scheduler_UnitTestCase
from .unit.test_worker import Worker_UnitTestCase
from .unit.test_worker_assistant import WorkerAssistant_UnitTestCase

# Components
from ..components.test.unit.test_config import Config_UnitTestCase
from ..components.test.unit.test_dynamo_db import dynamodb_client_UnitTestCase
from ..components.test.unit.test_helpers import helpers_UnitTestCase
from sosw.components.test.unit.test_siblings import siblings_TestCase
from sosw.components.test.unit.test_sns import sns_TestCase

# Managers
from ..managers.test.unit.test_task import *
from ..managers.test.unit.test_ecology import *

def suite():
    test_suite = unittest.TestSuite()

    # Core applications
    test_suite.addTest(unittest.makeSuite(app_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Labourer_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Orchestrator_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Scavenger_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Scheduler_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Worker_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(WorkerAssistant_UnitTestCase))

    # Components
    test_suite.addTest(unittest.makeSuite(Config_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(dynamodb_client_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(helpers_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(siblings_TestCase))
    test_suite.addTest(unittest.makeSuite(sns_TestCase))

    # Managers
    test_suite.addTest(unittest.makeSuite(ecology_manager_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(task_manager_UnitTestCase))

    return test_suite


if __name__ == '__main__':
    mySuit = suite()

    runner = unittest.TextTestRunner()
    runner.run(mySuit)
