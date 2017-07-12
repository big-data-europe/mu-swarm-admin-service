from muswarmadmin.prefixes import SwarmUI

from tests.integration.helpers import IntegrationTestCase, unittest_run_loop


class PipelinesTestCase(IntegrationTestCase):
    @unittest_run_loop
    async def test_pipeline_removal(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        await self.app.sparql.update(
            """
            # NOTE: temporarily use the alternative syntax because DELETE WHERE
            #       is not handled by the delta service
            #DELETE WHERE {
            #    GRAPH {{graph}} {
            #        {{pipeline_iri}} ?p ?o
            #    }
            #}
            WITH {{graph}}
            DELETE {
                {{pipeline_iri}} ?p ?o
            }
            WHERE {
                {{pipeline_iri}} ?p ?o
            }
            """, pipeline_iri=pipeline_iri)
        await self.scheduler_complete(pipeline_id)
        self.assertFalse(self.project_exists(pipeline_id))

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

    @unittest_run_loop
    async def test_pipeline_actions(self):
        pipeline_iri, pipeline_id = await self.create_pipeline()
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Up)
        await self.restart_action(pipeline_iri, pipeline_id)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Stopped)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Started)
        await self.do_action(pipeline_iri, pipeline_id, SwarmUI.Down)
