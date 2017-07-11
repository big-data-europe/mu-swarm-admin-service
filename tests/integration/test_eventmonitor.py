import subprocess
from aiosparql.syntax import Node, RDF

import muswarmadmin.eventmonitor
import muswarmadmin.main
from muswarmadmin.prefixes import Dct, Doap, Mu, SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class BaseEventMonitorTestCase(IntegrationTestCase):
    async def get_application(self):
        app = muswarmadmin.main.Application()
        app.sparql_timeout = self.sparql_timeout
        app.on_cleanup.append(muswarmadmin.main.stop_action_schedulers)
        app.on_startup.append(muswarmadmin.main.start_event_monitor)
        app.on_cleanup.append(muswarmadmin.main.stop_event_monitor)
        app.on_cleanup.append(muswarmadmin.main.stop_cleanup)
        app.on_startup.append(muswarmadmin.eventmonitor.startup)
        return app

    async def prepare_database(self):
        await super().prepare_database()
        repository_id = self.uuid4()
        repository_iri = self.resource("repositories", repository_id)
        pipeline_id = self.uuid4()
        pipeline_iri = self.resource("pipeline-instances", pipeline_id)
        await self.prepare_node(
            Node(repository_iri, {
                RDF.type: Doap.GitRepository,
                Mu.uuid: repository_id,
                Doap.location: self.example_repo,
                SwarmUI.pipelines: Node(pipeline_iri, [
                    (RDF.type, SwarmUI.Pipeline),
                    (Mu.uuid, pipeline_id),
                    (SwarmUI.status, SwarmUI.Error),
                ]),
            }))

        subprocess.check_call(["git", "clone", self.example_repo, pipeline_id],
                              cwd="/data")

        self.pipeline_iri = pipeline_iri
        self.pipeline_id = pipeline_id


class DatabaseUpdatedEventMonitorTestCase(BaseEventMonitorTestCase):
    async def prepare_database(self):
        await super().prepare_database()
        self.node1_id = self.uuid4()
        self.node1_iri = self.resource("services", self.node1_id)
        await self.prepare_node(
            Node(self.pipeline_iri, {
                SwarmUI.services: Node(self.node1_iri, {
                    RDF.type: SwarmUI.Service,
                    Mu.uuid: self.node1_id,
                    SwarmUI.scaling: 3,  # an invalid value
                    SwarmUI.status: SwarmUI.Started,  # an invalid value
                    Dct.title: "service1",
                }),
            }))

    @unittest_run_loop
    async def test_database_updated(self):

        result = await self.describe(self.node1_iri)
        self.assertEqual(result[self.node1_iri][SwarmUI.scaling][0]['value'],
                         0)
        self.assertEqual(result[self.node1_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Stopped)
        result = await self.describe(self.pipeline_iri)
        self.assertEqual(result[self.pipeline_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Stopped)


class DockerUpdatedEventMonitorTestCase(BaseEventMonitorTestCase):
    async def prepare_database(self):
        await super().prepare_database()
        self.node2_id = self.uuid4()
        self.node2_iri = self.resource("services", self.node2_id)
        await self.prepare_node(
            Node(self.pipeline_iri, {
                SwarmUI.services: Node(self.node2_iri, {
                    RDF.type: SwarmUI.Service,
                    Mu.uuid: self.node2_id,
                    SwarmUI.scaling: 0,  # an invalid value
                    SwarmUI.status: SwarmUI.Stopped,  # an invalid value
                    Dct.title: "service2",
                }),
            }))
        subprocess.check_call(["docker-compose", "up", "-d", "service2"],
                              cwd="/data/%s" % self.pipeline_id)

    @unittest_run_loop
    async def test_docker_updated(self):
        result = await self.describe(self.node2_iri)
        self.assertEqual(result[self.node2_iri][SwarmUI.scaling][0]['value'],
                         1)
        self.assertEqual(result[self.node2_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Started)
        result = await self.describe(self.pipeline_iri)
        self.assertEqual(result[self.pipeline_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Started)
