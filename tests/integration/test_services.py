from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import TestCase, unittest_run_loop


class ServicesTestCase(TestCase):
    async def do_action(self, pipeline_id, service_iri, service_id, action,
                        pending_state):
        await self.insert_triples([
            (service_iri, SwarmUI.requestedStatus, action),
        ])
        await self.scheduler_complete(pipeline_id)
        result = await self.describe(service_iri)
        self.assertTrue(result and result[service_iri])
        self.assertEqual(result[service_iri][SwarmUI.status][0]['value'],
                         pending_state)

    async def restart_action(self, pipeline_id, service_iri, service_id):
        await self.insert_triples([
            (service_iri, SwarmUI.restartRequested, "true"),
        ])
        await self.scheduler_complete(pipeline_id)
        result = await self.describe(service_iri)
        self.assertTrue(result and result[service_iri])
        self.assertEqual(result[service_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Restarting)

    async def scale_action(self, pipeline_id, service_iri, service_id, value):
        await self.insert_triples([
            (service_iri, SwarmUI.requestedScaling, value),
        ])
        await self.scheduler_complete(pipeline_id)
        result = await self.describe(service_iri)
        self.assertTrue(result and result[service_iri])
        self.assertEqual(result[service_iri][SwarmUI.status][0]['value'],
                         SwarmUI.Scaling)

    @unittest_run_loop
    async def test_service_actions(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        services = await self.get_services(pipeline_id)
        service_iri, service_id = next(iter(services.values()))
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.do_action(pipeline_id, service_iri, service_id,
                             SwarmUI.Stopped, SwarmUI.Stopping)
        await self.do_action(pipeline_id, service_iri, service_id,
                             SwarmUI.Started, SwarmUI.Starting)
        await self.restart_action(pipeline_id, service_iri, service_id)
        await self.scale_action(pipeline_id, service_iri, service_id, 2)
