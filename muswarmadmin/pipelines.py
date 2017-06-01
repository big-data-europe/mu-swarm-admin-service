from aiosparql.syntax import IRI, Literal
from asyncio import ensure_future
import git
import logging
from shutil import rmtree

from muswarmadmin.prefixes import Mu, SwarmUI


logger = logging.getLogger(__name__)


async def shutdown_and_cleanup_pipeline(app, project_id):
    logger.info("Shutting down and cleaning up pipeline %s", project_id)
    try:
        repo = git.Repo('/data/%s' % project_id)
    except git.exc.NoSuchPathError:
        return
    await app.run_command(
        "docker-compose", "down", cwd="/data/%s" % project_id)
    rmtree(repo.working_dir)


_state_to_action = {
    SwarmUI.Up: (["up", "-d"], SwarmUI.Starting, SwarmUI.Up),
    SwarmUI.Down: (["down"], SwarmUI.Stopping, SwarmUI.Down),
    SwarmUI.Stopped: (["stop"], SwarmUI.Stopping, SwarmUI.Stopped),
}


async def do_action(app, project_id, args, end_state):
    logger.info("Changing pipeline %s status to %s", project_id, end_state)
    await app.run_command("docker-compose", *args, cwd="/data/%s" % project_id)
    if end_state == SwarmUI.Up:
        await app.join_public_network(project_id)
        await app.restart_proxy()
    await app.update_state(project_id, end_state)


async def restart_action(app, project_id):
    logger.info("Restarting pipeline %s", project_id)
    await app.run_command("docker-compose", "restart",
                          cwd="/data/%s" % project_id)
    await app.update_state(project_id, SwarmUI.Up)


async def update(app, inserts, deletes):
    for subject, triples in inserts.items():
        for triple in triples:
            if triple.p == SwarmUI.requestedStatus:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                project_id = await app.get_resource_id(subject)
                if triple.o in _state_to_action:
                    args, pending_state, end_state = _state_to_action[triple.o]
                    await app.update_state(project_id, pending_state)
                    ensure_future(do_action(app, project_id, args, end_state))
                else:
                    logger.error("Requested status not implemented: %s",
                                 triple.o.value)
                # we don't want any other action on this pipeline
                break
            elif triple.p == SwarmUI.restartRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                project_id = await app.get_resource_id(subject)
                await app.reset_restart_requested(project_id)
                await app.update_state(project_id, SwarmUI.Restarting)
                ensure_future(restart_action(app, project_id))
                # we don't want any other action on this pipeline
                break

    for subject, triples in deletes.items():
        for triple in triples:
            if triple.p == Mu.uuid:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                ensure_future(
                    shutdown_and_cleanup_pipeline(app, triple.o.value))
            elif triple.p == SwarmUI.services:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                await app.sparql.update("""
                    DELETE WHERE {
                        GRAPH {{graph}} {
                            {{subject}} ?p ?o
                        }
                    }""", subject=triple.o)
