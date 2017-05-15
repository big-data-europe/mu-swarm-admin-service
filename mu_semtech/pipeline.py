from compose.service import ImageType
from flask import current_app
from flask_restful import Resource, abort
from flask_restful.reqparse import RequestParser
import git
from os import environ as ENV
from shutil import rmtree
from uuid import uuid1, uuid4

from mu_semtech.helpers import (
    check_permissions, find_user, get_project, get_resource_id, open_project)
from sparql import client, graph
from sparql.escape import escape_string
from sparql.prefixes import doap, swarmui


PIPELINE_CREATION_TOKEN = ENV.get('PIPELINE_CREATION_TOKEN')
PIPELINE_MANAGEMENT_TOKEN = ENV.get('PIPELINE_MANAGEMENT_TOKEN')


def update_state(uuid, state):
    query_template = """
        WITH <%(graph)s>
        DELETE {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:status
            ?state
        }
        WHERE {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:status
            ?state
        }
        INSERT {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:status
            %(new_state)s
        }
        """
    client.ensure_update(query_template % {
        'graph': graph,
        'uuid': uuid,
        'new_state': state,
    })


def reset_restart_requested(uuid):
    query_template = """
        WITH <%(graph)s>
        DELETE {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:restartRequested
            ?state
        }
        WHERE {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:restartRequested
            ?state
        }
        INSERT {
            <http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%(uuid)s>
            swarmui:restartRequested
            "false"
        }
        """
    client.ensure_update(query_template % {
        'graph': graph,
        'uuid': uuid,
    })


def update_pipelines(pipelines):
    for subject, triples in pipelines.items():
        for triple in triples:
            if triple.p == swarmui.get("requestedStatus"):
                project_id = get_resource_id(subject)
                project = open_project(project_id)
                if triple.o == swarmui.get("Up"):
                    update_state(project.name, 'swarmui:Starting')
                    project.up()
                    update_state(project.name, 'swarmui:Up')
                elif triple.o == swarmui.get("Down"):
                    update_state(project.name, 'swarmui:Stopping')
                    project.down(ImageType.none, True)
                    update_state(project.name, 'swarmui:Down')
                elif triple.o == swarmui.get("Stopped"):
                    update_state(project.name, 'swarmui:Stopping')
                    project.stop()
                    update_state(project.name, 'swarmui:Stopped')
                else:
                    current_app.logger.exception(
                        "Not implemented action: %s" % triple.o.value)
            elif triple.p == swarmui.get("restartRequested") and triple.o == "true":
                project_id = get_resource_id(subject)
                project = open_project(project_id)
                reset_restart_requested(project.name)
                update_state(project.name, 'swarmui:Restarting')
                project.restart()
                update_state(project.name, 'swarmui:Up')


def update_repositories(repositories):
    for subject, triples in repositories.items():
        for triple in triples:
            if triple.p == swarmui.get("pipelines"):
                data = client.ensure_query("""
                    WITH <http://mu.semte.ch/application>
                    DESCRIBE <%s>
                    """ % subject)
                assert data
                repository, = tuple(data.values())
                location = repository[doap + 'location'][0]['value']
                project_id = get_resource_id(triple.o.value)
                try:
                    existing_repo = git.Repo('/data/%s' % project_id)
                except git.exc.NoSuchPathError:
                    pass
                else:
                    abort(409,
                        title="pipeline already exists",
                        detail="pipeline %s already exists" % project_id)
                repo = git.Repo.init('/data/%s' % project_id)
                repo.create_remote('origin', location)
                _pull_repository(repo)
                try:
                    _update_database(repository, project_id)
                except Exception:
                    rmtree(repo.working_dir)
                    raise
                else:
                    return project_id


def _pull_repository(repo):
    try:
        repo.remotes.origin.fetch()
        repo.create_head('master', repo.remotes.origin.refs.master)\
            .set_tracking_branch(repo.remotes.origin.refs.master)
        repo.heads.master.checkout()
    except Exception:
        current_app.logger.exception(
            "can not pull from the repository %s"
            % repo.remotes.origin.url)
        rmtree(repo.working_dir)
        abort(400,
            title="can not pull from the repository",
            detail="can not pull from the repository: %s"
            % repo.remotes.origin.url)


def _create_base_pipeline(repository, repository_id, project_id):
    pipeline_iriref = \
        "<http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%s>" % project_id
    triples = [
        (pipeline_iriref, "mu:uuid", escape_string(project_id)),
        (pipeline_iriref, "swarmui:status", "swarmui:Down"),
        (pipeline_iriref, "rdf:type", "swarmui:Pipeline"),
        ("<http://swarm-ui.big-data-europe.eu/resources/repositories/%s>"
            % repository_id, "swarmui:pipelines", pipeline_iriref),
    ]
    if PIPELINE_MANAGEMENT_TOKEN:
        grant_id = str(uuid4())
        grant_iriref = \
            "<http://swarm-ui.big-data-europe.eu/resources/grant/%s>" % grant_id
        triples.extend([
            (grant_iriref, "mu:uuid", escape_string(grant_id)),
            (grant_iriref, "auth:hasToken",
                "<%s>" % PIPELINE_MANAGEMENT_TOKEN),
            (grant_iriref, "auth:operatesOn", pipeline_iriref),
            ("<%s>" % find_user(), "auth:hasRight", grant_iriref),
        ])
    if 'http://mu.semte.ch/vocabularies/ext/mdlIcon' in repository:
        triples.append((
            pipeline_iriref,
            "ext:mdlIcon",
            escape_string(
                repository['http://mu.semte.ch/vocabularies/ext/mdlIcon']\
                [0]['value'])
        ))
    if 'https://www.w3.org/1999/xhtml/vocab#icon' in repository:
        triples.append((
            pipeline_iriref,
            "w3vocab:icon",
            escape_string(
                repository['https://www.w3.org/1999/xhtml/vocab#icon']\
                [0]['value'])
        ))
    client.ensure_update("""
        INSERT DATA {
            GRAPH <%(graph)s> {
                %(triples)s
            }
        }
        """ % {
            'graph': graph,
            'triples': " . ".join("%s %s %s" % x for x in triples),
        })


def _update_database(repository, project_id):
    project = open_project(project_id)
    pipeline_iriref = \
        "<http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/%s>" % project_id
    triples = []
    for service in project.services:
        service_id = str(uuid4())
        service_iriref = (
            "<http://swarm-ui.big-data-europe.eu/resources/services/%s>"
            % service_id)
        triples.extend([
            (service_iriref, "mu:uuid", escape_string(service_id)),
            (service_iriref, "dct:title",
                escape_string(service.name)),
            (service_iriref, "swarmui:scaling", 1),
            (service_iriref, "rdf:type", "swarmui:Service"),
            (pipeline_iriref, "swarmui:services", service_iriref),
        ])
    client.ensure_update("""
        INSERT DATA {
            GRAPH <%(graph)s> {
                %(triples)s
            }
        }
        """ % {
            'graph': graph,
            'triples': " . ".join("%s %s %s" % x for x in triples),
        })


def create_pipeline(repository_id):
    check_permissions(
        PIPELINE_CREATION_TOKEN, repository_id,
        title="access forbidden",
        detail="you are not allowed to create pipelines")
    data = client.ensure_query("""
        WITH <http://mu.semte.ch/application>
        DESCRIBE ?x
        WHERE {
            ?x mu:uuid %s
        }
        """ % escape_string(repository_id))
    if not data:
        abort(404,
            title="repository not found",
            detail="repository %s not found" % repository_id)
    repository, = tuple(data.values())
    location = repository['http://usefulinc.com/ns/doap#location'][0]['value']
    project_id = str(uuid1(0))
    try:
        existing_repo = git.Repo('/data/%s' % project_id)
    except git.exc.NoSuchPathError:
        pass
    else:
        abort(409,
            title="pipeline already exists",
            detail="pipeline %s already exists" % project_id)
    repo = git.Repo.init('/data/%s' % project_id)
    repo.create_remote('origin', location)
    _pull_repository(repo)
    try:
        _create_base_pipeline(repository, repository_id, project_id)
        _update_database(repository, project_id)
    except Exception:
        rmtree(repo.working_dir)
        raise
    else:
        return project_id


class PipelineList(Resource):
    def post(self, repository_id=None):
        project_id = create_pipeline(repository_id)
        return {
            'data': {
                'id': project_id,
                'type': 'pipelines',
            },
        }


class BasePipelineResource(Resource):
    def check_permissions(self, pipeline_id):
        check_permissions(
            PIPELINE_MANAGEMENT_TOKEN, pipeline_id,
            title="access forbidden",
            detail="you are not allowed to manage this pipeline")


class PipelineUp(BasePipelineResource):
    @get_project
    def post(self):
        self.check_permissions(self.project.name)
        update_state(self.project.name, 'swarmui:Starting')
        self.project.up()
        update_state(self.project.name, 'swarmui:Up')
        return {'status': 'ok'}


class PipelineDown(BasePipelineResource):
    @get_project
    def post(self):
        print(self.project.name)
        self.check_permissions(self.project.name)
        update_state(self.project.name, 'swarmui:Stopping')
        self.project.down(ImageType.none, True)
        update_state(self.project.name, 'swarmui:Down')
        return {'status': 'ok'}


class PipelineStop(BasePipelineResource):
    @get_project
    def post(self):
        self.check_permissions(self.project.name)
        update_state(self.project.name, 'swarmui:Stopping')
        self.project.stop()
        update_state(self.project.name, 'swarmui:Stopped')
        return {'status': 'ok'}


class PipelineRestart(BasePipelineResource):
    @get_project
    def post(self):
        self.check_permissions(self.project.name)
        reset_restart_requested(project.name)
        update_state(self.project.name, 'swarmui:Restarting')
        self.project.restart()
        update_state(self.project.name, 'swarmui:Up')
        return {'status': 'ok'}
