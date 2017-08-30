import os

from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class PipelinesTestCase(IntegrationTestCase):
    @unittest_run_loop
    async def test_pipeline_removal(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        await self.insert_triples([
            (pipeline_iri, SwarmUI.deleteRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        self.assertFalse(self.project_exists(pipeline_id))
        await self.assertNotExists(s=pipeline_iri)
        await self.assertNotExists(o=pipeline_iri)

    async def do_action(self, pipeline_iri, pipeline_id, action):
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, action),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, action)

    async def up_action(self, pipeline_iri, pipeline_id):
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)

    async def down_action(self, pipeline_iri, pipeline_id):
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Down),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Stopped)

    async def restart_action(self, pipeline_iri, pipeline_id):
        await self.insert_triples([
            (pipeline_iri, SwarmUI.restartRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        await self.assertNotExists(s=pipeline_iri, p=SwarmUI.restartRequested)

    async def update_action(self, pipeline_iri, pipeline_id):
        old_services = await self.get_services(pipeline_id)
        await self.insert_triples([
            (pipeline_iri, SwarmUI.updateRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        new_services = await self.get_services(pipeline_id)
        # NOTE: the services have been replaced so their UUID and their IRI
        #       has changed but the name of the services remain the same
        self.assertEqual(set(old_services.keys()), set(new_services.keys()))
        self.assertNotEqual(set(old_services.values()),
                            set(new_services.values()))
        await self.assertStatus(pipeline_iri, SwarmUI.Up)
        await self.assertNotExists(s=pipeline_iri, p=SwarmUI.updateRequested)
        await self.assertExists(s=pipeline_iri, p=SwarmUI.services)

    @unittest_run_loop
    async def test_pipeline_actions(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Up)
        await self.restart_action(pipeline_iri, pipeline_id)
        await self.update_action(pipeline_iri, pipeline_id)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Stopped)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Started)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Down)

    @unittest_run_loop
    async def test_is_last_pipeline(self):
        repository_iri, repository_id = await self.create_repository()
        pipeline1_iri, pipeline1_id = \
            await self.create_pipeline(repository_iri=repository_iri)
        self.assertTrue(await self.app.is_last_pipeline(pipeline1_id))
        pipeline2_iri, pipeline2_id = \
            await self.create_pipeline(repository_iri=repository_iri)
        self.assertFalse(await self.app.is_last_pipeline(pipeline1_id))
        self.assertFalse(await self.app.is_last_pipeline(pipeline2_id))
        await self.insert_triples([
            (pipeline1_iri, SwarmUI.deleteRequested, "true"),
        ])
        await self.scheduler_complete(pipeline1_id)
        self.assertTrue(await self.app.is_last_pipeline(pipeline2_id))

    async def _to_remove__create_pipeline_with_compose_yaml(self):
        from aiosparql.syntax import Node, RDF
        from muswarmadmin.prefixes import Mu, SwarmUI
        pipeline_id = self.uuid4()
        pipeline_iri = self.resource("pipeline-instances", pipeline_id)
        pipeline_node = Node(pipeline_iri, {
            RDF.type: SwarmUI.Pipeline,
            Mu.uuid: pipeline_id,
            SwarmUI.composeYaml: self.compose_yaml,
        })
        await self.db.update(
            "INSERT DATA { GRAPH {{graph}} { {{}} } }", pipeline_node)
        res = await self.db.query("""
            SELECT ?s ?p ?o
            FROM {{graph}}
            WHERE {
                ?s ?p ?o .
                FILTER ( ?s = {{pipeline_iri}} )
            }
            """, pipeline_iri=pipeline_iri)
        delta = [{
            'graph': self.db.graph.value,
            'inserts': res['results']['bindings'],
            'deletes': [],
        }]
        res = await self.client.post("/update", json={'delta': delta})
        self.assertLess(res.status, 400, await res.text())
        await self.scheduler_complete(pipeline_id)
        return (pipeline_iri, pipeline_id)

    @unittest_run_loop
    async def test_docker_compose_yaml(self):
        if False:  # delta service handles multiline strings
            pipeline_iri, pipeline_id = \
                await self.create_pipeline(compose=True)
        else:  # delta service broken
            pipeline_iri, pipeline_id = \
                await self._to_remove__create_pipeline_with_compose_yaml()
        result = await self.describe(pipeline_iri)
        self.assertTrue(result and result[pipeline_iri])
        self.assertIn(SwarmUI.services, result[pipeline_iri])
        self.assertTrue(self.project_exists(pipeline_id))
        self.assertEqual(result[pipeline_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Down)

    @unittest_run_loop
    async def test_docker_compose_yaml_update(self):
        if False:  # delta service handles multiline strings
            pipeline_iri, pipeline_id = \
                await self.create_pipeline(compose=True)
        else:  # delta service broken
            pipeline_iri, pipeline_id = \
                await self._to_remove__create_pipeline_with_compose_yaml()
        compose_path = "/data/%s/docker-compose.yml" % pipeline_id
        old_services = await self.get_services(pipeline_id)
        old_mtime = os.stat(compose_path).st_mtime
        await self.insert_triples([
            (pipeline_iri, SwarmUI.updateRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        new_services = await self.get_services(pipeline_id)
        new_mtime = os.stat(compose_path).st_mtime
        # NOTE: the services have been replaced so their UUID and their IRI
        #       has changed but the name of the services remain the same
        self.assertEqual(set(old_services.keys()), set(new_services.keys()))
        self.assertNotEqual(set(old_services.values()),
                            set(new_services.values()))
        await self.assertStatus(pipeline_iri, SwarmUI.Up)
        await self.assertNotExists(s=pipeline_iri, p=SwarmUI.updateRequested)
        await self.assertExists(s=pipeline_iri, p=SwarmUI.services)
        self.assertNotEqual(old_mtime, new_mtime)
