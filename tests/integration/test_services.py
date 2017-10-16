import subprocess

from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class ServicesTestCase(IntegrationTestCase):
    async def do_action(self, pipeline_id, service_iri, service_id, action):
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
        await self.assertNode(service_iri, {
            SwarmUI.status: SwarmUI.Started,
            SwarmUI.scaling: value,
        })

    @unittest_run_loop
    async def test_actions(self):
        print("==================== test_actions ====================")
        repository_iri, repository_id = await self.create_repository()
        drc_iri, drc_id = \
            await self.create_drc_node(repository_iri=repository_iri)
        pipeline_iri, pipeline_id = await self.create_pipeline(repository_iri=repository_iri)
        services = await self.get_services(pipeline_id)
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Up)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Stopped)
        await self.assertStatus(pipeline_iri, SwarmUI.Stopped)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Started)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        await self.restart_action(pipeline_id, service_iri, service_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        await self.scale_action(pipeline_id, service_iri, service_id, 2)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Killed)
        await self.assertStatus(pipeline_iri, SwarmUI.Stopped)
        for service_iri, service_id in services.values():
            await self.do_action(pipeline_id, service_iri, service_id,
                                 SwarmUI.Removed)

    @unittest_run_loop
    async def test_manual_actions(self):
        print("==================== test_manual_actions ====================")
        repository_iri, repository_id = await self.create_repository()
        drc_iri, drc_id = \
            await self.create_drc_node(repository_iri=repository_iri)
        pipeline_iri, pipeline_id = await self.create_pipeline(repository_iri=repository_iri)
        await self.insert_triples([
            (pipeline_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Up)
        project_path = "/data/%s" % pipeline_id
        subprocess.check_call(["docker-compose", "stop"], cwd=project_path)
        await self.wait_scheduler(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Stopped)
        subprocess.check_call(["docker-compose", "start"], cwd=project_path)
        await self.wait_scheduler(pipeline_id)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)

    @unittest_run_loop
    async def test_up_action(self):
        print("==================== test_up_action ====================")
        repository_iri, repository_id = await self.create_repository()
        drc_iri, drc_id = \
            await self.create_drc_node(repository_iri=repository_iri)
        pipeline_iri, pipeline_id = await self.create_pipeline(repository_iri=repository_iri)
        services = await self.get_services(pipeline_id)
        service_iri, service_id = services["service1"]
        await self.insert_triples([
            (service_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(service_iri, SwarmUI.Up)
        await self.assertStatus(pipeline_iri, SwarmUI.Started)

    @unittest_run_loop
    async def test_logs(self):
        print("==================== test_logs ====================")
        repository_iri, repository_id = await self.create_repository()
        drc_iri, drc_id = \
            await self.create_drc_node(repository_iri=repository_iri)
        pipeline_iri, pipeline_id = await self.create_pipeline(repository_iri=repository_iri)
        services = await self.get_services(pipeline_id)
        service_iri, service_id = services["service1"]
        await self.insert_triples([
            (service_iri, SwarmUI.requestedStatus, SwarmUI.Up),
        ])
        await self.scheduler_complete(pipeline_id)
        await self.assertStatus(service_iri, SwarmUI.Up)
        async with self.client.get(f"/services/{service_id}/logs") as request:
            self.assertEqual(request.status, 200)
        async with self.client.get(f"/services/invalid/logs") as request:
            self.assertEqual(request.status, 404)
