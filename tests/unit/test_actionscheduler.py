import asyncio
import unittest
from aiohttp.test_utils import (
    setup_test_loop, teardown_test_loop, unittest_run_loop)

from muswarmadmin.actionscheduler import (
    ActionScheduler, OneActionScheduler, StopScheduler)


class BaseActionSchedulerTestCase(unittest.TestCase):
    as_class = ActionScheduler

    def setUp(self):
        if self.as_class.executers:
            raise RuntimeError("The executers list is not empty")
        self.loop = setup_test_loop()
        self.job_values = []

    def tearDown(self):
        self.loop.run_until_complete(self.as_class.graceful_cancel())
        teardown_test_loop(self.loop)

    async def _job_test(self, value, side_effect, sleep):
        if sleep:
            await asyncio.sleep(sleep)
        self.job_values.append(value)
        if side_effect is not None:
            raise side_effect

    async def enqueue(self, value, side_effect=None, sleep=0):
        await self.as_class.execute("test", self._job_test,
                                    [value, side_effect, sleep],
                                    loop=self.loop)

    @property
    def executers(self):
        return self.as_class.executers

    @property
    def executer(self):
        return self.as_class.executers['test'].executer

    async def cancel(self):
        await self.as_class.executers['test'].cancel()


class ActionSchedulerTestCase(BaseActionSchedulerTestCase):
    @unittest_run_loop
    async def test_stop_scheduler(self):
        await self.enqueue("foo", side_effect=StopScheduler())
        await self.executer
        self.assertNotIn("test", self.executers)
        self.assertEqual(self.job_values, ["foo"])

    @unittest_run_loop
    async def test_cancel_scheduler(self):
        await self.enqueue("foo")
        await self.cancel()
        self.assertNotIn("test", self.executers)
        self.assertEqual(self.job_values, ["foo"])

    @unittest_run_loop
    async def test_cancel_honors_all_jobs_and_order(self):
        await self.enqueue(1, sleep=1)
        await self.enqueue(2, sleep=0.1)
        await self.enqueue(3, sleep=0.4)
        await self.cancel()
        self.assertEqual(self.job_values, [1, 2, 3])

    @unittest_run_loop
    async def test_graceful_cancel_all_executers(self):
        await self.as_class.execute("test1", self._job_test,
                                    [1, None, 0], loop=self.loop)
        await self.as_class.execute("test1", self._job_test,
                                    [2, None, 0], loop=self.loop)
        await self.as_class.execute("test2", self._job_test,
                                    [1, None, 0], loop=self.loop)
        await self.as_class.execute("test2", self._job_test,
                                    [2, None, 0], loop=self.loop)
        await self.as_class.graceful_cancel()
        self.assertEqual(len(self.executers), 0)

    @unittest_run_loop
    async def test_executers_are_resilient(self):
        await self.enqueue("foo")
        await self.enqueue("bar", side_effect=RuntimeError("test exception"))
        await self.enqueue("baz")
        await self.cancel()
        self.assertEqual(self.job_values, ["foo", "bar", "baz"])


class OneActionSchedulerTestCase(BaseActionSchedulerTestCase):
    as_class = OneActionScheduler

    @unittest_run_loop
    async def test_all_enqueued_actions_are_dropped(self):
        await self.enqueue("foo", sleep=1)
        await self.enqueue("bar", sleep=1)
        await self.enqueue("baz", sleep=1)
        await self.cancel()
        self.assertEqual(self.job_values, ["foo"])
