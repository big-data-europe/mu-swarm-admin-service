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
        self.assertEqual(old_services.keys(), new_services.keys())
        self.assertNotEqual(old_services.values(), new_services.values())
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
        print("==================== test_is_last_pipeline ====================")
        repository_iri, repository_id = await self.create_repository()
        drc_iri, drc_id = \
            await self.create_drc_node(repository_iri=repository_iri)
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
