from muswarmadmin.actionscheduler import StopScheduler
from muswarmadmin.pipelines import shutdown_and_cleanup_pipeline

from tests.unit.helpers import UnitTestCase, unittest_run_loop


class PipelinesTestCase(UnitTestCase):
    @unittest_run_loop
    async def test_pipeline_removal_already_removed(self):
        with self.assertRaises(StopScheduler):
            await shutdown_and_cleanup_pipeline(self.app, "does_not_exist")
