import asyncio
import os
import shutil
import socket
import subprocess
import uuid
from aiohttp.test_utils import (
    AioHTTPTestCase, TestClient, TestServer, setup_test_loop,
    unittest_run_loop)
from aiosparql.client import SPARQLClient
from aiosparql.syntax import escape_string, IRI, Node, RDF, Triples
from yarl import URL

import muswarmadmin.delta
import muswarmadmin.main
from muswarmadmin.actionscheduler import ActionScheduler
from muswarmadmin.prefixes import Doap, Mu, SwarmUI

__all__ = ['TestCase', 'unittest_run_loop']


# NOTE: temporary fix: ensure a child watcher is set before running test
def setup_test_loop(loop_factory=asyncio.new_event_loop):  # noqa
    """Create and return an asyncio.BaseEventLoop
    instance.

    The caller should also call teardown_test_loop,
    once they are done with the loop.
    """
    loop = loop_factory()
    asyncio.set_event_loop(None)
    policy = asyncio.get_event_loop_policy()
    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    policy.set_child_watcher(watcher)
    return loop


class FixedPortTestServer(TestServer):
    @asyncio.coroutine
    def start_server(self, loop=None, **kwargs):
        if self.server:
            return
        self._loop = loop
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("0.0.0.0", 80))
        self.port = self._socket.getsockname()[1]
        self._ssl = None
        self.scheme = 'http'
        self._root = URL('{}://{}:{}'.format(self.scheme,
                                             self.host,
                                             self.port))

        handler = yield from self._make_factory(**kwargs)
        self.server = yield from self._loop.create_server(
            handler, ssl=self._ssl, sock=self._socket)

    # NOTE: temporary fix: ensure that the event loop is set in the app before
    #       firing on_startup
    @asyncio.coroutine
    def _make_factory(self, **kwargs):
        self.app._set_loop(self._loop)  # here
        yield from self.app.startup()
        self.handler = self.app.make_handler(loop=self._loop, **kwargs)
        return self.handler


class TestCase(AioHTTPTestCase):
    example_repo = \
        "https://github.com/cecton/docker-compose-for-testing.git"
    sparql_timeout = 5

    async def get_application(self):
        # NOTE: disable the Docker event monitor on purpose. We have no way
        #       at this point to know when all the operations will be finished
        #       and it may interfere with the state inside the database
        app = muswarmadmin.main.Application()
        app.sparql_timeout = self.sparql_timeout
        app.on_cleanup.append(muswarmadmin.main.stop_action_schedulers)
        app.on_cleanup.append(muswarmadmin.main.stop_cleanup)
        app.router.add_post("/update", muswarmadmin.delta.update)
        return app

    async def scheduler_complete(self, key):
        if key not in ActionScheduler.executers:
            raise KeyError("ActionScheduler for key %s does not exist" % key)
        await ActionScheduler.executers[key].cancel()

    def uuid4(self):
        return str(uuid.uuid4()).replace("-", "").upper()

    def resource(self, type_, id):
        return (
            muswarmadmin.main.Application.base_resource + "%s/%s" % (type_, id)
        )

    def project_exists(self, project_name):
        return os.path.exists("/data/%s" % project_name)

    async def prepare_triples(self, triples):
        await self.db.update(
            "INSERT DATA { GRAPH {{graph}} { {{}} } }", Triples(triples))

    async def insert_triples(self, triples):
        await self.app.sparql.update(
            "INSERT DATA { GRAPH {{graph}} { {{}} } }", Triples(triples))

    async def prepare_node(self, node):
        await self.prepare_triples([node])

    async def insert_node(self, node):
        await self.insert_triples([node])

    async def describe(self, subject):
        return await self.app.sparql.query("DESCRIBE {{}} FROM {{graph}}",
                                           subject)

    async def create_pipeline(self, location=None):
        if location is None:
            location = self.example_repo
        repository_id = self.uuid4()
        repository_iri = self.resource("repositories", repository_id)
        pipeline_id = self.uuid4()
        pipeline_iri = self.resource("pipeline-instances", pipeline_id)
        await self.insert_node(Node(repository_iri, {
            RDF.type: Doap.GitRepository,
            Mu.uuid: repository_id,
            Doap.location: location,
            SwarmUI.pipelines: Node(pipeline_iri, {
                RDF.type: SwarmUI.Pipeline,
                Mu.uuid: pipeline_id,
            }),
        }))
        await self.scheduler_complete(pipeline_id)
        return (pipeline_iri, pipeline_id)

    async def get_services(self, project_name):
        result = await self.app.sparql.query(
            """
            SELECT ?name ?service ?uuid
            FROM {{graph}}
            WHERE {
                ?pipeline mu:uuid {{}} ;
                  swarmui:services ?service .

                ?service mu:uuid ?uuid ;
                  dct:title ?name .
            }
            """, escape_string(project_name))
        return {
            x['name']['value']: (IRI(x['service']['value']),
                                 x['uuid']['value'])
            for x in result['results']['bindings']
        }

    async def prepare_database(self):
        await self.db.update("CLEAR GRAPH {{graph}}")

    def setUp(self):
        self.loop = setup_test_loop()

        self.db = SPARQLClient(endpoint="http://database:8890/sparql",
                               loop=self.loop,
                               read_timeout=self.sparql_timeout)
        self.loop.run_until_complete(self.prepare_database())

        self.app = self.loop.run_until_complete(self.get_application())

        self.server = FixedPortTestServer(self.app)
        self.client = self.loop.run_until_complete(
            self._get_client(self.server))

        self.loop.run_until_complete(self.client.start_server())

    def tearDown(self):
        self.loop.run_until_complete(self.db.close())
        super().tearDown()
        for project_name in os.listdir("/data"):
            project_path = "/data/%s" % project_name
            subprocess.call(["docker-compose", "down"], cwd=project_path)
            shutil.rmtree(project_path)

    # NOTE: temporary fix, will be fixed with the next aiohttp release
    @asyncio.coroutine
    def _get_client(self, app):
        """Return a TestClient instance."""
        return TestClient(app, loop=self.loop)
