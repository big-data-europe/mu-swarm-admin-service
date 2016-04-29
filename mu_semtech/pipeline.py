from compose.service import ImageType
from flask_restful import Resource, abort, inputs
from flask_restful.reqparse import RequestParser
from flask_restful_sparql.http import Client
from flask_restful_sparql.escaping import escape_string
import git
import logging
from os import environ as ENV
from uuid import uuid1, uuid4

from mu_semtech.helpers import get_project, open_project


PREFIXES = """
PREFIX swarmui: <http://swarmui.semte.ch/vocabularies/core/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX doap: <http://usefulinc.com/ns/doap#>
PREFIX w3vocab: <http://https://www.w3.org/1999/xhtml/vocab#>
"""

endpoint_url = ENV.get('MU_SPARQL_ENDPOINT', 'http://database:8890/sparql')
graph = ENV.get('MU_APPLICATION_GRAPH', 'http://mu.semte.ch/application')
client = Client(endpoint_url)
client.logger.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


def _ensure_get_query(query):
    resp = client.get_query("\n".join([PREFIXES, query]))
    if resp.status_code >= 300:
        logger.error("Database query failed: %s", resp.text)
        abort(500)
    return resp.json()


def _ensure_post_query(query):
    resp = client.post_query("\n".join([PREFIXES, query]))
    if resp.status_code >= 300:
        logger.error("Database query failed: %s", resp.text)
        abort(500)
    return resp.json()


def _update_state(uuid, state):
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
    _ensure_post_query(query_template % {
        'graph': graph,
        'uuid': uuid,
        's_uuid': escape_string(uuid),
        'new_state': state,
    })


class PipelineList(Resource):
    parser = RequestParser()
    parser.add_argument('repository_id', required=True)

    def post(self):
        args = self.parser.parse_args()
        data = _ensure_get_query("""
            WITH <http://mu.semte.ch/application>
            DESCRIBE ?x
            WHERE {
                ?x mu:uuid %s
            }
            """ % escape_string(args['repository_id']))
        if not data:
            abort(404,
                title="repository not found",
                detail="repository %s not found" % args['repository_id'])
        repository, = tuple(data.values())
        location = repository['http://usefulinc.com/ns/doap#location'][0]['value']
        project_id = str(uuid1())
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
        try:
            repo.remotes.origin.fetch()
            repo.create_head('master', repo.remotes.origin.refs.master)\
                .set_tracking_branch(repo.remotes.origin.refs.master)
            repo.heads.master.checkout()
            repo.remotes.origin.pull()
            project = open_project(project_id)
            for service in project.services:
                service_id = str(uuid4())
                _ensure_post_query("""
                    INSERT DATA {
                        GRAPH <%(graph)s> {
                            <http://swarmui.semte.ch/resources/services/%(uuid)s>
                            mu:uuid
                            %(s_uuid)s .
                            <http://swarmui.semte.ch/resources/services/%(uuid)s>
                            dct:title
                            %(title)s .
                            <http://swarmui.semte.ch/resources/services/%(uuid)s>
                            swarmui:scaling
                            1 .
                            <http://swarmui.semte.ch/resources/services/%(uuid)s>
                            <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
                            swarmui:Service
                        }
                    }
                    """ % {
                        'graph': graph,
                        'title': escape_string(service.name),
                        'uuid': service_id,
                        's_uuid': escape_string(service_id),
                    })
            _ensure_post_query("""
                INSERT DATA {
                    GRAPH <%(graph)s> {
                        <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
                        mu:uuid
                        %(s_uuid)s .
                        <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
                        swarmui:status
                        swarmui:Inactive .
                        <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
                        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
                        swarmui:Pipeline .
                        <http://swarmui.semte.ch/resources/repositories/%(repository_id)s>
                        swarmui:instances
                        <http://swarmui.semte.ch/resources/pipelines/%(uuid)s>
                    }
                }
                """ % {
                    'graph': graph,
                    'uuid': project_id,
                    's_uuid': escape_string(project_id),
                    'repository_id': args['repository_id'],
                })
        except Exception:
            current_app.logger.exception(
                "can not pull from the repository %s"
                % repo.remotes.origin.url)
            try:
                abort(400,
                    title="can not pull from the repository",
                    detail="can not pull from the repository: %s"
                    % repo.remotes.origin.url)
            finally:
                rmtree(repo.working_dir)
        else:
            return {
                'data': {
                    'id': project_id,
                    'type': 'pipelines',
                },
            }


class PipelineUp(Resource):
    @get_project
    def post(self):
        _update_state(self.project.name, 'swarmui:Starting')
        self.project.up()
        _update_state(self.project.name, 'swarmui:Up')
        return {'status': 'ok'}


class PipelineDown(Resource):
    @get_project
    def post(self):
        _update_state(self.project.name, 'swarmui:Stopping')
        self.project.down(ImageType.none, True)
        _update_state(self.project.name, 'swarmui:Down')
        return {'status': 'ok'}


class PipelineStop(Resource):
    @get_project
    def post(self):
        _update_state(self.project.name, 'swarmui:Stopping')
        self.project.stop()
        _update_state(self.project.name, 'swarmui:Stopped')
        return {'status': 'ok'}
