import aiodockerpy
import logging
import os
from aiosparql.syntax import escape_string, IRI, Literal
from shutil import rmtree

from muswarmadmin.prefixes import SwarmUI
from muswarmadmin.actionscheduler import StopScheduler


logger = logging.getLogger(__name__)


async def remove_docker_images(app, project_id):
    data = app.open_compose_data(project_id)
    for image in set([x['image'] for x in data.services]):
        try:
            await app.docker.remove_image(image)
        except aiodockerpy.errors.APIError as exc:
            if exc.is_server_error():
                logger.error(str(exc))


async def shutdown_and_cleanup_pipeline(app, project_id):
    """
    Shutdown a pipeline with docker-compose down, then remove the entire
    PIPELINE source directory
    """
    logger.info("Shutting down and cleaning up pipeline %s", project_id)
    project_path = "/data/%s" % project_id
    if not os.path.exists(project_path):
        raise StopScheduler()
    await app.update_state(project_id, SwarmUI.Removing)
    await app.run_compose("down", cwd="/data/%s" % project_id)
    if await app.is_last_pipeline(project_id):
        await remove_docker_images(app, project_id)
    rmtree(project_path)
    await app.sparql.update("""
        # NOTE: DELETE WHERE is not handled by the Delta service
        #DELETE WHERE {
        #    GRAPH {{graph}} {
        #        ?pipeline mu:uuid {{project_id}} ;
        #          swarmui:services ?service ;
        #          ?p1 ?o1 .
        #
        #        ?service ?p2 ?o2 .
        #
        #        ?repository swarmui:pipelines ?pipeline .
        #    }
        #}
        WITH {{graph}}
        DELETE {
            ?pipeline mu:uuid {{project_id}} ;
              swarmui:services ?service ;
              ?p1 ?o1 .

            ?service ?p2 ?o2 .

            ?repository swarmui:pipelines ?pipeline .
        }
        WHERE {
            ?pipeline mu:uuid {{project_id}} ;
              swarmui:services ?service ;
              ?p1 ?o1 .

            ?service ?p2 ?o2 .

            ?repository swarmui:pipelines ?pipeline .
        }
        """, project_id=escape_string(project_id))
    raise StopScheduler()


_state_to_action = {
    SwarmUI.Down: (["down"], SwarmUI.Stopping),
    SwarmUI.Started: (["start"], SwarmUI.Starting),
    SwarmUI.Stopped: (["stop"], SwarmUI.Stopping),
}


async def do_action(app, project_id, args, pending_state, end_state):
    """
    Action triggered for any change of swarmui:requestedStatus but swarmui:Up
    """
    logger.info("Changing pipeline %s status to %s", project_id, end_state)
    await app.update_state(project_id, pending_state)
    proc = await app.run_compose(*args, cwd="/data/%s" % project_id)
    if proc.returncode is not 0:
        await app.update_state(project_id, SwarmUI.Error)
    else:
        await app.update_state(project_id, end_state)


async def up_action(app, project_id):
    """
    Action triggered when swarmui:requestedStatus change
    """
    logger.info("Changing pipeline %s status to %s", project_id, SwarmUI.Up)
    await app.update_state(project_id, SwarmUI.Starting)
    proc = await app.run_compose("up", "-d", cwd="/data/%s" % project_id,
                                 timeout=app.compose_up_timeout)
    if proc.returncode is not 0:
        await app.update_state(project_id, SwarmUI.Error)
    else:
        await app.update_state(project_id, SwarmUI.Up)


async def restart_action(app, project_id):
    """
    Action triggered when swarmui:restartRequested has become true
    """
    logger.info("Restarting pipeline %s", project_id)
    await app.update_state(project_id, SwarmUI.Restarting)
    await app.run_compose("restart", cwd="/data/%s" % project_id)
    await app.update_state(project_id, SwarmUI.Started)


async def describe(app, subject):
    return await app.sparql.query("DESCRIBE {{}} FROM {{graph}}",
                                       subject)

async def update_action(app, project_id, pipeline):
    """
    Action triggered when swarmui:updateRequested has become true
    """
    logger.info("Updating pipeline %s", project_id)
    await app.update_state(project_id, SwarmUI.Updating)
    print("--------------------- Pipeline changed to Updating successfully")
    proc = await app.run_command("git", "fetch", cwd="/data/%s" % project_id)
    print("--------------------- Git fetch done successfully")
    if proc.returncode != 0:
        print("--------------------- Git fetch /data BOUM")
        await app.update_state(project_id, SwarmUI.Error)
        return
    proc = await app.run_command("git", "reset", "--hard", "origin/master",
                                 cwd="/data/%s" % project_id)
    if proc.returncode != 0:
        print("--------------------- Git reset --hard /data BOUM")
        await app.update_state(project_id, SwarmUI.Error)
        return
    print("--------------------- Git reset done successfully")
    proc = await app.run_compose("pull", cwd="/data/%s" % project_id)
    if proc.returncode is not 0:
        print("--------------------- Drc pull BOUM")
        await app.update_state(project_id, SwarmUI.Error)
        return
    print("--------------------- Drc pull done successfully")
    await app.update_pipeline_services(pipeline)
    proc = await app.run_compose("up", "-d", "--remove-orphans",
                                 cwd="/data/%s" % project_id)
    if proc.returncode is not 0:
        print("--------------------- Git drc up -d /data BOUM")
        await app.update_state(project_id, SwarmUI.Error)
    else:
        print("--------------------- drc up done successfully")
        await app.update_state(project_id, SwarmUI.Up)
        # If I print the status here, it is Up
        result = await describe(app, pipeline)
        print(result) # will print Started O_o



async def update(app, inserts, deletes):
    """
    Handler for the updates of the pipelines received by the Delta service
    """
    logger.debug("Receiving updates: inserts=%r deletes=%r", inserts, deletes)
    for subject, triples in inserts.items():
        for triple in triples:

            if triple.p == SwarmUI.requestedStatus:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                project_id = await app.get_resource_id(subject)
                await app.enqueue_action(
                    project_id, app.remove_triple,
                    [project_id, SwarmUI.requestedStatus])
                if triple.o == SwarmUI.Up:
                    await app.enqueue_action(project_id, up_action,
                                             [app, project_id])
                elif triple.o in _state_to_action:
                    args, pending_state = _state_to_action[triple.o]
                    await app.enqueue_action(
                        project_id, do_action,
                        [app, project_id, args, pending_state, triple.o])
                else:
                    logger.error("Requested status not implemented: %s",
                                 triple.o.value)

            elif triple.p == SwarmUI.restartRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                project_id = await app.get_resource_id(subject)
                await app.enqueue_action(
                    project_id, app.remove_triple,
                    [project_id, SwarmUI.restartRequested])
                await app.enqueue_action(project_id, restart_action,
                                         [app, project_id])

            elif triple.p == SwarmUI.deleteRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                project_id = await app.get_resource_id(subject)
                await app.enqueue_action(
                    project_id, app.remove_triple,
                    [project_id, SwarmUI.deleteRequested])
                await app.enqueue_action(
                    project_id, shutdown_and_cleanup_pipeline,
                    [app, project_id])

            elif triple.p == SwarmUI.updateRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                project_id = await app.get_resource_id(subject)
                await app.enqueue_action(
                    project_id, app.remove_triple,
                    [project_id, SwarmUI.updateRequested])
                await app.enqueue_action(
                    project_id, update_action, [app, project_id, triple.s])


async def get_existing_updates(sparql):
    return await sparql.query(
        """
        SELECT ?s ?p ?o
        FROM {{graph}}
        WHERE
        {
            ?s a swarmui:Pipeline ;
              ?p ?o .
            FILTER (?p IN (
              swarmui:restartRequested,
              swarmui:requestedStatus
            ))
        }
        """)
