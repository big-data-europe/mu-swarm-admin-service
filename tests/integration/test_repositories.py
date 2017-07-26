from muswarmadmin.actionscheduler import ActionScheduler
from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class RepositoriesTestCase(IntegrationTestCase):
    @unittest_run_loop
    async def test_initialization(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        result = await self.describe(pipeline_iri)
        self.assertTrue(result and result[pipeline_iri])
        self.assertIn(SwarmUI.services, result[pipeline_iri])
        self.assertTrue(self.project_exists(pipeline_id))

    @unittest_run_loop
    async def test_invalid_repository(self):
        pipeline_iri, pipeline_id = await self.create_pipeline(location="/")
        result = await self.describe(pipeline_iri)
        self.assertTrue(result and result[pipeline_iri])
        self.assertNotIn(SwarmUI.services, result[pipeline_iri])
        self.assertFalse(self.project_exists(pipeline_id))

    @unittest_run_loop
    async def test_no_repository(self):
        pipeline_iri, pipeline_id = await self.create_pipeline(location=None)
        result = await self.describe(pipeline_iri)
        self.assertTrue(result and result[pipeline_iri])
        self.assertNotIn(SwarmUI.services, result[pipeline_iri])
        self.assertFalse(self.project_exists(pipeline_id))

    @unittest_run_loop
    async def test_pipeline_removal(self):
        repository_iri, repository_id = await self.create_repository()
        pipeline_iri, pipeline_id = \
            await self.create_pipeline(repository_iri=repository_iri)
        await self.insert_triples([
            (repository_iri, SwarmUI.deleteRequested, "true"),
        ])
        await self.scheduler_complete(repository_id)
        await self.assertNotExists(s=repository_iri)
        self.assertNotIn(pipeline_id, ActionScheduler.executers)
        self.assertFalse(self.project_exists(pipeline_id))
        await self.assertNotExists(s=pipeline_iri)
