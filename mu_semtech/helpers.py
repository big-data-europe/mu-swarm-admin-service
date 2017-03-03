import compose
from compose import config
from compose.config.environment import Environment
from compose.const import API_VERSIONS
from compose.project import Project
import docker
from flask import current_app, request
from flask_restful import abort
from functools import wraps
import git
import logging
from os import path

from sparql import client, graph
from sparql.escape import escape_string


CONFIG_FILES = ['docker-compose.yml', 'docker-compose.prod.yml']

client.logger.setLevel(logging.DEBUG)


def get_resource_id(subject):
    query_template = """
        SELECT ?o
        FROM <%(graph)s>
        WHERE
        {
            <%(subject)s> mu:uuid ?o .
        }
        """
    result = client.ensure_query(query_template % {
        'graph': graph,
        'subject': subject,
    })
    return result['results']['bindings'][0]['o']['value']


def get_resource_title(subject):
    query_template = """
        SELECT ?o
        FROM <%(graph)s>
        WHERE
        {
            <%(subject)s> dct:title ?o .
        }
        """
    result = client.ensure_query(query_template % {
        'graph': graph,
        'subject': subject,
    })
    return result['results']['bindings'][0]['o']['value']


def get_service_pipeline(subject):
    query_template = """
        SELECT ?s
        FROM <%(graph)s>
        WHERE
        {
            ?s a swarmui:Pipeline ; swarmui:services <%(subject)s> .
        }
        """
    result = client.ensure_query(query_template % {
        'graph': graph,
        'subject': subject,
    })
    return result['results']['bindings'][0]['s']['value']


def open_project(project_id):
    project_dir = '/data/%s' % project_id
    config_files = filter(
        lambda x: path.exists(path.join(project_dir, x)),
        CONFIG_FILES)
    environment = Environment.from_env_file(project_dir)
    config_details = config.find(project_dir, config_files, environment)
    config_data = config.load(config_details)
    api_version = environment.get(
        'COMPOSE_API_VERSION',
        API_VERSIONS[config_data.version])
    client = docker.api.APIClient(
        version=api_version,
        **docker.utils.kwargs_from_env(environment=environment))
    return Project.from_config(project_id, config_data, client)


def get_repository(func):
    @wraps(func)
    def wrapper(self, **kwargs):
        assert not hasattr(self, 'project')
        try:
            self.repo = git.Repo(
                '/data/%s' % kwargs['project_id'])
        except git.exc.NoSuchPathError:
            abort(404,
                title="no such project",
                detail="can not find project %s" % kwargs['project_id'])
        return func(self, **kwargs)
    return wrapper


def get_project(func):
    @wraps(func)
    def wrapper(self, **kwargs):
        assert not hasattr(self, 'project')
        project_id = kwargs.pop('project_id', None)
        try:
            self.project = open_project(project_id)
        except config.errors.ComposeFileNotFound:
            abort(404,
                title="no such pipeline",
                detail="can not find pipeline %s" % project_id)
        return func(self, **kwargs)
    return wrapper


def get_service(func):
    @wraps(func)
    def wrapper(self, **kwargs):
        assert not hasattr(self, 'project') and not hasattr(self, 'service')
        self.service_id = kwargs.pop('service_id')
        data = client.ensure_query("""
            DESCRIBE ?x {?x mu:uuid %s}
            """ % escape_string(self.service_id))
        if not data:
            abort(404,
                title="no such service",
                detail="can not find service %s" % self.service_id)
        service_iri = \
            "http://swarmui.semte.ch/resources/services/" + self.service_id
        service_name = data[service_iri]['http://purl.org/dc/terms/title'][0]['value']
        pipeline_iri = next(filter(
            lambda x: x.startswith(
                'http://swarmui.semte.ch/resources/pipelines'),
            data.keys()))
        project_id = pipeline_iri.rsplit('/', 1)[1]
        self.project = open_project(project_id)
        self.service = self.project.get_service(service_name)
        return func(self, **kwargs)
    return wrapper


def check_permissions(token, id, **error_members):
    if token is None:
        return
    session_iri = request.headers.get('mu-session-id')
    assert session_iri, "missing header mu-session-id"
    res = client.ensure_query("""
        WITH <http://mu.semte.ch/application>
        ASK {
            <%(session_iri)s> session:account/^foaf:account/((a/auth:belongsToActorGroup*/auth:hasRight)|(auth:belongsToActorGroup*/auth:hasRight)) ?tokenConnection .
            ?tokenConnection auth:hasToken <%(token)s> .
            ?tokenConnection auth:operatesOn/((^auth:belongsToArtifactGroup*/^a)|(^auth:belongsToArtifactGroup*))/mu:uuid %(id)s
        }
        """ % {
            'session_iri': session_iri,
            'token': token,
            'id': escape_string(id),
        })
    if not res['boolean']:
        abort(403, **error_members)


def find_user():
    session_iri = request.headers.get('mu-session-id')
    assert session_iri, "missing header mu-session-id"
    data = client.ensure_query("""
        WITH <http://mu.semte.ch/application>
        SELECT *
        WHERE {
            <%s>
            <http://mu.semte.ch/vocabularies/session/account>/^foaf:account
            ?x
        }
        """ % session_iri)
    return data['results']['bindings'][0]['x']['value']
