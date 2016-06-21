import compose
from compose import config
from compose.config.environment import Environment
from compose.const import API_VERSIONS
from compose.project import Project
import docker
from flask import current_app
from flask_restful import abort
from flask_restful_sparql.http import Client
from flask_restful_sparql.escaping import escape_string
from functools import wraps
import git
import logging
from os import environ as ENV, path


PREFIXES = """
PREFIX swarmui: <http://swarmui.semte.ch/vocabularies/core/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX doap: <http://usefulinc.com/ns/doap#>
PREFIX w3vocab: <https://www.w3.org/1999/xhtml/vocab#>
"""
CONFIG_FILES = ['docker-compose.yml', 'docker-compose.prod.yml']

endpoint_url = ENV.get('MU_SPARQL_ENDPOINT', 'http://database:8890/sparql')
graph = ENV.get('MU_APPLICATION_GRAPH', 'http://mu.semte.ch/application')
client = Client(endpoint_url)
client.logger.setLevel(logging.DEBUG)


def ensure_get_query(query):
    resp = client.get_query("\n".join([PREFIXES, query]))
    if resp.status_code >= 300:
        current_app.logger.error("Database query failed: %s", resp.text)
        abort(500)
    return resp.json()


def ensure_post_query(query):
    resp = client.post_query("\n".join([PREFIXES, query]))
    if resp.status_code >= 300:
        current_app.logger.error("Database query failed: %s", resp.text)
        abort(500)
    return resp.json()


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
    client = docker.Client(
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
        data = ensure_get_query("""
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
