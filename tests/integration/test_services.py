from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class ServicesTestCase(IntegrationTestCase):
    async def do_action(self, pipeline_id, service_iri, service_id, action,
                        pending_state):
        await self.insert_triples([
            (service_iri, SwarmUI.requestedStatus, action),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(service_iri, action)

    async def restart_action(self, pipeline_id, service_iri, service_id):
        await self.insert_triples([
            (service_iri, SwarmUI.restartRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(service_iri, SwarmUI.Started)

    async def scale_action(self, pipeline_id, service_iri, service_id, value):
        await self.insert_triples([
            (service_iri, SwarmUI.requestedScaling, value),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertNode(service_iri, [
            (SwarmUI.status, SwarmUI.Started),
            (SwarmUI.scaling, value),
        ])

    @unittest_run_loop
    async def test_actions(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        services = await self.get_services(pipeline_id)
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Stopped, SwarmUI.Stopping)
        await self.assertStatus(pipeline_iri, SwarmUI.Stopped)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Started, SwarmUI.Starting)
        await self.restart_action(pipeline_id, service_iri, service_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        await self.scale_action(pipeline_id, service_iri, service_id, 2)
