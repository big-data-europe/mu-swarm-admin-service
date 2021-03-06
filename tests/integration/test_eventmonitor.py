import os
import subprocess
import sys
from aiosparql.syntax import Node, RDF

from muswarmadmin.prefixes import Dct, Doap, Mu, SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class BaseEventMonitorTestCase(IntegrationTestCase):
    async def prepare_database(self):
        await super().prepare_database()
        repository_id = self.uuid4()
        repository_iri = self.resource("stacks", repository_id)
        pipeline_id = self.uuid4()
        pipeline_iri = self.resource("pipeline-instances", pipeline_id)
        await self.prepare_node(
            Node(repository_iri, {
                RDF.type: Doap.Stack,
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


def test_docker_not_reachable():
    env = dict(os.environ, DOCKER_HOST="unix:///var/run/nowhere", ENV="dev")
    proc = subprocess.run([sys.executable, "-m", "muswarmadmin"], env=env)
    assert proc.returncode == 1


def test_database_not_reachable():
    env = dict(os.environ, MU_SPARQL_ENDPOINT="http://nowhere", ENV="dev")
    proc = subprocess.run([sys.executable, "-m", "muswarmadmin"], env=env)
    assert proc.returncode == 1


def test_database_not_answering_properly():
    env = dict(os.environ, MU_SPARQL_ENDPOINT="http://example.org", ENV="dev")
    proc = subprocess.run([sys.executable, "-m", "muswarmadmin"], env=env)
    assert proc.returncode == 1
