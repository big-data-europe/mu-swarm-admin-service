import logging
import os
from aiosparql.syntax import IRI, Literal
from shutil import rmtree

import muswarmadmin.pipelines
from muswarmadmin.prefixes import Doap, SwarmUI

logger = logging.getLogger(__name__)


async def repository_has_location(app, pipeline):
    """
    Check if the repository associated with the pipeline
    has a location (git url).
    """
    result = await app.sparql.query("""
        ASK
        FROM {{graph}}
        WHERE {
         ?repository swarmui:pipelines {{pipeline}} .
         ?repository doap:location ?location .
        }
        """, pipeline=pipeline)
    return result['boolean']


async def get_repository_drc(app, pipeline):
    """
    Get DockerCompose file associated with a given Pipeline.
    """
    result = await app.sparql.query("""
        SELECT DISTINCT ?drctext
        WHERE {
         ?repository swarmui:pipelines {{pipeline}} .
         ?repository swarmui:dockerComposeFile ?drc .
         ?drc stackbuilder:text ?drctext .
        }
        """, pipeline=pipeline)
    if not result['results']['bindings'] or \
            not result['results']['bindings'][0]:
        raise KeyError("The pipeline %r does not have a drc" % pipeline)
    return result['results']['bindings'][0]['drctext']['value']


async def initialize_pipeline(app, pipeline, project_id, location, branch):
    """
    Action triggered when a new pipeline appear in the database: clone the
    sources, insert the triples for services
    """
    logger.info("Initializing pipeline %s", project_id)
    project_path = "/data/%s" % project_id
    if os.path.exists(project_path):
        logger.error("Pipeline at %s already exists", project_path)
        return
    await app.update_state(project_id, SwarmUI.Initializing)

    if await repository_has_location(app, pipeline=pipeline):
        proc = await app.run_command(
            "git", "clone", location, "-b", (branch or "master"), project_id,
            cwd="/data")
        if proc.returncode != 0:
            logger.error("Failed to clone repository at %s", location)
            if os.path.exists(project_path):
                rmtree(project_path)
            return
    else:
        os.makedirs(project_path)

    repository_drc = await get_repository_drc(app, pipeline=pipeline)
    drc_path = os.path.join(project_path, "docker-compose.yml")
    with open(drc_path, "w") as df:
        df.write(repository_drc)

    try:
        await app.update_pipeline_services(pipeline)
    except Exception:
        rmtree(project_path)
        raise
    else:
        await app.update_state(project_id, SwarmUI.Down)


async def remove_repository(app, repository):
    """
    Remove a repository by shutting down and removing all the associated
    pipelines then remove the repository itself
    """
    logger.info("Removing repository %s", repository)
    result = await app.sparql.query(
        """
        SELECT *
        FROM {{graph}}
        WHERE {
            {{}} swarmui:pipelines ?pipeline .
            ?pipeline mu:uuid ?uuid .
        }
        """, repository)
    if not result['results']['bindings'] or \
            not result['results']['bindings'][0]:
        logger.debug("No pipeline for repository %s", repository)
        return
    pipelines = [
        data['uuid']['value']
        for data in result['results']['bindings']
    ]
    for pipeline_id in pipelines:
        await app.enqueue_action(
            pipeline_id, muswarmadmin.pipelines.shutdown_and_cleanup_pipeline,
            [app, pipeline_id])
    for pipeline_id in pipelines:
        await app.wait_action(pipeline_id)
    await app.sparql.update(
        """
        # NOTE: DELETE WHERE is not handled by the Delta service
        WITH {{graph}}
        DELETE {
            {{repository}} ?p ?o
        }
        WHERE {
            {{repository}} ?p ?o
        }
        """, repository=repository)


async def update(app, inserts, deletes):
    """
    Handler for the updates of the repositories received by the Delta service
    """
    logger.debug("Receiving updates: inserts=%r deletes=%r", inserts, deletes)
    for subject, triples in inserts.items():
        for triple in triples:

            if triple.p == SwarmUI.pipelines:
                assert isinstance(triple.o, IRI), \
                    "wrong type: %r" % type(triple.o)
                result = await app.sparql.query("DESCRIBE {{}} FROM {{graph}}",
                                                subject)
                info, = tuple(result.values())
                location = info.get(Doap.location, [{'value': ''}])[0]['value']
                branch = info.get(SwarmUI.branch, [{'value': ''}])[0]['value']
                repository_id = await app.get_resource_id(subject)
                project_id = await app.get_resource_id(triple.o)
                await app.enqueue_action(project_id, initialize_pipeline, [
                    app, triple.o, project_id, location, branch,
                ])

            elif triple.p == SwarmUI.deleteRequested:
                assert isinstance(triple.o, Literal), \
                    "wrong type: %r" % type(triple.o)
                if not triple.o == "true":
                    continue
                repository_id = await app.get_resource_id(subject)
                await app.enqueue_action(repository_id, remove_repository,
                                         [app, subject])


async def get_existing_updates(sparql):
    return await sparql.query(
        """
        SELECT ?s (swarmui:pipelines AS ?p) ?o
        FROM {{graph}}
        WHERE
        {
            ?s a doap:Stack ;
              swarmui:pipelines ?o .
            ?o a swarmui:Pipeline .
            FILTER (NOT EXISTS {?o swarmui:status ?status})
        }
        """)
