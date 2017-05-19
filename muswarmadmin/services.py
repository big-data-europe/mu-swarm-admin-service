from aiohttp import web
from aiosparql.escape import escape_any
from aiosparql.syntax import Literal
from asyncio import ensure_future
import logging

from muswarmadmin.prefixes import SwarmUI


MAXIMUM_LINE_OF_LOGS = 1000
logger = logging.getLogger(__name__)


async def restart_action(app, project_id, service_id):
    logger.info("Restarting service %s", service_id)
    service_name = await app.get_dct_title(service_id)
    await app.run_command(
        "docker-compose", "restart", service_name, cwd="/data/%s" % project_id)
    await app.update_state(service_id, SwarmUI.Up)


async def scaling_action(app, project_id, service_id, value):
    logger.info("Scaling service %s to %s", service_id, value)
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
    await app.update_state(service_id, SwarmUI.Up)


async def update(app, inserts, deletes):
    for subject, triples in inserts.items():
        for triple in triples:
            if triple.p == SwarmUI.restartRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.reset_restart_requested(service_id)
                await app.update_state(service_id, SwarmUI.Restarting)
                ensure_future(restart_action(app, project_id, service_id))
                # we don't want any other action on this service
                break
            elif triple.p == SwarmUI.scaling:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                service_id = await app.get_resource_id(subject)
                project_id = await app.get_service_pipeline(service_id)
                await app.update_state(service_id, SwarmUI.Scaling)
                ensure_future(scaling_action(app, project_id, service_id,
                                             int(triple.o.value)))
                # we don't want any other action on this service
                break


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
