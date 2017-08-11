import uuid
from aiohttp.test_utils import AioHTTPTestCase, TestServer, unittest_run_loop
from aiosparql.test_utils import TestSPARQLClient

import muswarmadmin.main
from muswarmadmin import delta

__all__ = ['UnitTestCase', 'unittest_run_loop']


class Application(muswarmadmin.main.Application):
    @property
    def sparql(self):
        if not hasattr(self, '_sparql'):
            self._sparql = TestSPARQLClient(TestServer(self),
                                            endpoint="/",
                                            graph="http://example.org",
                                            loop=self.loop)
        return self._sparql

    @property
    def docker(self):
        raise RuntimeError("Can not do Docker queries during unit test")


class UnitTestCase(AioHTTPTestCase):
    async def get_application(self):
        app = Application(loop=self.loop)
        app.router.add_post("/update", delta.update)
        await app.sparql.start_server()
        return app

    def tearDown(self):
        self.loop.run_until_complete(self.app.sparql.close())
        super().tearDown()

    def uuid4(self):
        return str(uuid.uuid4()).replace("-", "").upper()
