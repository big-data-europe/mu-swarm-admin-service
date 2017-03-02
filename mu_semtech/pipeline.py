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
from sparql.prefixes import swarmui


PIPELINE_CREATION_TOKEN = ENV.get('PIPELINE_CREATION_TOKEN')
PIPELINE_MANAGEMENT_TOKEN = ENV.get('PIPELINE_MANAGEMENT_TOKEN')


def update_state(uuid, state):
    query_template = """
        WITH <%(graph)s>
        DELETE {
            <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
            swarmui:status
            ?state
        }
        INSERT {
            <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
            swarmui:status
            %(new_state)s
        }
        WHERE {
            <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
            swarmui:status
            ?state
        }
        """
    client.ensure_update(query_template % {
        'graph': graph,
        'uuid': uuid,
        'new_state': state,
    })


def update_pipelines(pipelines):
    for subject, triples in pipelines.items():
        for triple in triples:
            if triple.p == swarmui.get("status"):
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
                elif triple.o == swarmui.get("Restarting"):
                    update_state(project.name, 'swarmui:Restarting')
                    project.restart()
                    update_state(project.name, 'swarmui:Up')
                else:
                    current_app.logger.exception(
                        "Not implemented action: %s" % triple.o)


class PipelineList(Resource):
    def check_permissions(self, repository_id):
        if not PIPELINE_CREATION_TOKEN:
            return
        check_permissions(
            PIPELINE_CREATION_TOKEN, repository_id,
            title="access forbidden",
            detail="you are not allowed to create pipelines")

    def _pull_repository(self, repo):
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

    def _update_database(self, repository, repository_id, project_id):
        project = open_project(project_id)
        pipeline_iriref = \
            "<http://swarmui.semte.ch/resources/pipelines/%s>" % project_id
        triples = [
            (pipeline_iriref, "mu:uuid", escape_string(project_id)),
            (pipeline_iriref, "swarmui:status", "swarmui:Down"),
            (pipeline_iriref, "rdf:type", "swarmui:Pipeline"),
            ("<http://swarmui.semte.ch/resources/repositories/%s>"
                % repository_id, "swarmui:pipelines", pipeline_iriref),
        ]
        if PIPELINE_MANAGEMENT_TOKEN:
            grant_id = str(uuid4())
            grant_iriref = \
                "<http://swarmui.semte.ch/resources/grant/%s>" % grant_id
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
        for service in project.services:
            service_id = str(uuid4())
            service_iriref = (
                "<http://swarmui.semte.ch/resources/services/%s>"
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

    def post(self, repository_id=None):
        self.check_permissions(repository_id)
        data = client.ensure_update("""
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
        self._pull_repository(repo)
        try:
            self._update_database(repository, repository_id, project_id)
        except Exception:
            rmtree(repo.working_dir)
            raise
        else:
            return {
                'data': {
                    'id': project_id,
                    'type': 'pipelines',
                },
            }


class BasePipelineResource(Resource):
    def check_permissions(self, pipeline_id):
        if not PIPELINE_MANAGEMENT_TOKEN:
            return
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
        update_state(self.project.name, 'swarmui:Restarting')
        self.project.restart()
        update_state(self.project.name, 'swarmui:Up')
        return {'status': 'ok'}
