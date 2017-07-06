from aiohttp import web
from aiosparql.syntax import IRI, Node, RDF, RDFTerm, Triples
import git
import logging
from shutil import rmtree
from uuid import uuid4

from muswarmadmin.prefixes import Dct, Doap, Mu, SwarmUI


logger = logging.getLogger(__name__)


async def _insert_triples(app, project_id, pipeline):
    """
    Generate and insert the triples about the services of a Docker Compose
    project (pipeline) inside the database
    """
    data = app.open_compose_data(project_id)
    triples = Triples()
    for service in data.services:
        service_id = uuid4()
        service_iri = RDFTerm(":%s" % service_id)
        triples.append((pipeline, SwarmUI.services, service_iri))
        triples.append(Node(service_iri, {
            Mu.uuid: service_id,
            Dct.title: service['name'],
            SwarmUI.scaling: 0,
            RDF.type: SwarmUI.Service,
            SwarmUI.status: SwarmUI.Stopped,
        }))
    await app.sparql.update("""
        PREFIX : {{services_iri}}

        INSERT DATA {
            GRAPH {{graph}} {
                {{triples}}
            }
        }""", services_iri=(app.base_resource + "services/"), triples=triples)


async def initialize_pipeline(app, pipeline, project_id, location, branch):
    """
    Action triggered when a new pipeline appear in the database: clone the
    sources, insert the triples for services
    """
    logger.info("Initializing pipeline %s", project_id)
    try:
        git.Repo('/data/%s' % project_id)
    except git.exc.NoSuchPathError:
        pass
    else:
        raise web.HTTPConflict(
            body="pipeline %s already exists" % project_id)
    repo = git.Repo.init('/data/%s' % project_id)
    repo.create_remote('origin', location)
    try:
        repo.remotes.origin.fetch()
        remote = repo.remotes.origin.refs[branch or 'master']
        head = repo.create_head(branch or 'master', remote)
        head.set_tracking_branch(remote)
        head.checkout()
    except Exception:
        logger.exception(
            "can not pull from the repository %s",
            repo.remotes.origin.url)
        rmtree(repo.working_dir)
        raise web.HTTPBadRequest(
            body="can not pull from the repository: %s"
                 % repo.remotes.origin.url)
    try:
        await _insert_triples(app, project_id, pipeline)
    except Exception:
        rmtree(repo.working_dir)
        raise
    else:
        await app.update_state(project_id, SwarmUI.Down)


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
                if not location:
                    logger.error("Pipeline %s: can not clone repository %s, "
                                 "location not specified",
                                 project_id, repository_id)
                    await app.update_state(project_id, SwarmUI.Error)
                    return
                await app.enqueue_action(project_id, initialize_pipeline, [
                    app, triple.o, project_id, location, branch,
                ])


async def get_existing_updates(sparql):
    return await sparql.query(
        """
        SELECT ?s (swarmui:pipelines AS ?p) ?o
        FROM {{graph}}
        WHERE
        {
            ?s a doap:GitRepository ;
              swarmui:pipelines ?o .
            ?o a swarmui:Pipeline .
            FILTER (NOT EXISTS {?o swarmui:status ?status})
        }
        """)
