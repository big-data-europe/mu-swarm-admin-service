import uuid
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiosparql.test_utils import TestSPARQLClient

import muswarmadmin.main
from muswarmadmin import delta

__all__ = ['UnitTestCase', 'unittest_run_loop']


class Application(muswarmadmin.main.Application):
    @property
    def sparql(self):
        if not hasattr(self, '_sparql'):
            self._sparql = TestSPARQLClient(self)
        return self._sparql

    @property
    def docker(self):
        raise RuntimeError("Can not do Docker queries during unit test")


class UnitTestCase(AioHTTPTestCase):
    async def get_application(self):
        app = Application()
        app.router.add_post("/update", delta.update)
        return app

    def uuid4(self):
        return str(uuid.uuid4()).replace("-", "").upper()
