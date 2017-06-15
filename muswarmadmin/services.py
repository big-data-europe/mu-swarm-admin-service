from aiohttp import web
from aiosparql.escape import escape_any
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
    logger.info("Changing service %s status to %s", service_id, end_state)
    await app.update_state(service_id, pending_state)
    service_name = await app.get_dct_title(service_id)
    all_args = list(args) + [service_name]
    proc = await app.run_command("docker-compose", *all_args,
                                 cwd="/data/%s" % project_id)
    if proc.returncode is not 0:
        await app.update_state(service_id, SwarmUI.Error)
    else:
        await app.update_state(service_id, end_state)


async def restart_action(app, project_id, service_id):
    logger.info("Restarting service %s", service_id)
    await app.update_state(service_id, SwarmUI.Restarting)
    service_name = await app.get_dct_title(service_id)
    await app.run_command("docker-compose", "restart", service_name,
                          cwd="/data/%s" % project_id)
    await app.update_state(service_id, SwarmUI.Started)


async def scaling_action(app, project_id, service_id, value):
    logger.info("Scaling service %s to %s", service_id, value)
    await app.update_state(service_id, SwarmUI.Scaling)
    service_name = await app.get_dct_title(service_id)
    await app.sparql.update("""
        WITH {{graph}}
        DELETE {
            ?s swarmui:scaling ?oldvalue
        }
        INSERT {
            ?s swarmui:scaling {{value}}
        }
        WHERE {
            ?s mu:uuid {{uuid}} .
            OPTIONAL { ?s swarmui:scaling ?oldvalue } .
        }""", uuid=escape_any(service_id), value=escape_any(value))
    await app.run_command(
        "docker-compose", "scale", "%s=%d" % (service_name, value),
        cwd="/data/%s" % project_id)
    await app.update_state(service_id, SwarmUI.Started)


async def update(app, inserts, deletes):
    for subject, triples in inserts.items():
        for triple in triples:
            if triple.p == SwarmUI.requestedStatus:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.reset_status_requested(service_id)
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
                await app.reset_restart_requested(service_id)
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


async def logs(request):
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
