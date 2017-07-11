import uuid
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

import muswarmadmin.main

__all__ = ['UnitTestCase', 'unittest_run_loop']


class UnitTestCase(AioHTTPTestCase):
    async def get_application(self):
        return muswarmadmin.main.Application()

    def uuid4(self):
        return str(uuid.uuid4()).replace("-", "").upper()
