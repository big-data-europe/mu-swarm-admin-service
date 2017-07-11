import logging
import os
from aiosparql.syntax import IRI, Literal
from shutil import rmtree

from muswarmadmin.prefixes import Mu, SwarmUI
from muswarmadmin.actionscheduler import StopScheduler


logger = logging.getLogger(__name__)


async def shutdown_and_cleanup_pipeline(app, project_id):
    """
    Shutdown a pipeline with docker-compose down, then remove the entire
    PIPELINE source directory
    """
    logger.info("Shutting down and cleaning up pipeline %s", project_id)
    project_path = "/data/%s" % project_id
    if not os.path.exists(project_path):
        raise StopScheduler()
    await app.run_command(
        "docker-compose", "down", cwd="/data/%s" % project_id)
    rmtree(project_path)
    raise StopScheduler()


async def delete_all_triples(app, subject):
    await app.sparql.update("""
        DELETE WHERE {
            GRAPH {{graph}} {
                {{}} ?p ?o
            }
        }""", subject)


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
    proc = await app.run_command("docker-compose", *args,
                                 cwd="/data/%s" % project_id)
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
    proc = await app.run_command("docker-compose", "up", "-d",
                                 cwd="/data/%s" % project_id,
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
    await app.run_command("docker-compose", "restart",
                          cwd="/data/%s" % project_id)
    await app.update_state(project_id, SwarmUI.Started)


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
                    project_id, app.reset_status_requested, [project_id])
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
                    project_id, app.reset_status_requested, [project_id])
                await app.enqueue_action(project_id, restart_action,
                                         [app, project_id])

    for subject, triples in deletes.items():
        uuid = None
        services_iri = []
        for triple in triples:
            if triple.p == Mu.uuid:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                uuid = triple.o.value
            elif triple.p == SwarmUI.services:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                services_iri.append(triple.o)
        if uuid:
            for service_iri in services_iri:
                await app.enqueue_action(uuid, delete_all_triples,
                                         [app, services_iri])
            await app.enqueue_action(
                uuid, shutdown_and_cleanup_pipeline,
                [app, uuid])


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
