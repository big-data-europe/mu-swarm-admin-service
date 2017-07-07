from aiohttp import web
from aiosparql.syntax import IRI, Literal
import logging

from muswarmadmin.prefixes import SwarmUI


MAXIMUM_LINE_OF_LOGS = 1000
logger = logging.getLogger(__name__)


_state_to_action = {
    SwarmUI.Started: (["start"], SwarmUI.Starting),
    SwarmUI.Stopped: (["stop"], SwarmUI.Stopping),
}


async def do_action(app, project_id, service_id,
                    args, pending_state, end_state):
    """
    Action triggered for any change of swarmui:requestedStatus
    """
    logger.info("Changing service %s status to %s", service_id, end_state)
    await app.update_state(service_id, pending_state)
    service_name = await app.get_dct_title(service_id)
    all_args = list(args) + [service_name]
    proc = await app.run_command("docker-compose", *all_args,
                                 cwd="/data/%s" % project_id)
    if proc.returncode is not 0:
        await app.update_state(service_id, SwarmUI.Error)


async def restart_action(app, project_id, service_id):
    """
    Action triggered when swarmui:restartRequested has become true
    """
    logger.info("Restarting service %s", service_id)
    await app.update_state(service_id, SwarmUI.Restarting)
    service_name = await app.get_dct_title(service_id)
    await app.run_command("docker-compose", "restart", service_name,
                          cwd="/data/%s" % project_id)


async def scaling_action(app, project_id, service_id, value):
    """
    Action triggered when swarmui:requestedScaling change
    """
    logger.info("Scaling service %s to %s", service_id, value)
    await app.update_state(service_id, SwarmUI.Scaling)
    service_name = await app.get_dct_title(service_id)
    await app.run_command(
        "docker-compose", "scale", "%s=%d" % (service_name, value),
        cwd="/data/%s" % project_id)


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
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.enqueue_action(
                    project_id, app.reset_status_requested, [service_id])
                if triple.o in _state_to_action:
                    args, pending_state = _state_to_action[triple.o]
                    await app.enqueue_action(
                        project_id, do_action,
                        [app, project_id, service_id, args, pending_state,
                         triple.o])
                else:
                    logger.error("Requested status not implemented: %s",
                                 triple.o.value)
            elif triple.p == SwarmUI.restartRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.enqueue_action(
                    project_id, app.reset_restart_requested, [service_id])
                await app.enqueue_action(project_id, restart_action,
                                         [app, project_id, service_id])
            elif triple.p == SwarmUI.requestedScaling:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.enqueue_action(
                    project_id, scaling_action,
                    [app, project_id, service_id, int(triple.o.value)])


async def get_existing_updates(sparql):
    return await sparql.query(
        """
        SELECT ?s ?p ?o
        FROM {{graph}}
        WHERE
        {
            ?s a swarmui:Service ;
              ?p ?o .
            FILTER (?p IN (
              swarmui:requestedScaling,
              swarmui:requestedStatus,
              swarmui:restartRequested
            ))
        }
        """)


async def logs(request):
    """
    API endpoint to fetch the logs of a container
    """
    service_id = request.match_info['id']
    try:
        project_id = await request.app.get_service_pipeline(service_id)
    except KeyError:
        raise web.HTTPNotFound(body="service %s not found" % service_id)
    service_name = await request.app.get_dct_title(service_id)
    proc = await request.app.run_command(
        "docker-compose", "logs", "--no-color",
        "--tail=%s" % MAXIMUM_LINE_OF_LOGS, service_name,
        cwd="/data/%s" % project_id, logging=False)
    if proc.returncode is None or proc.returncode < 0:
        raise web.HTTPRequestTimeout()
    else:
        logs = await proc.stdout.read()
        return web.Response(text=logs.decode())
