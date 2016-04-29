import compose
from compose import config
from compose.config.environment import Environment
from compose.const import API_VERSIONS
from compose.project import Project
from functools import wraps
import docker
from flask_restful import Resource, fields, inputs, marshal_with, abort
import git


def open_project(project_id):
    project_dir = '/data/%s' % project_id
    environment = Environment.from_env_file(project_dir)
    config_details = config.find(project_dir, None, environment)
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
        assert not hasattr(self, 'service')
        service_name = kwargs.pop('service_name', None)
        try:
            self.service = self.project.get_service(service_name)
        except compose.project.NoSuchService:
            abort(404,
                title="no such service",
                detail="can not find service %s" % service_name)
        return func(self, **kwargs)
    return wrapper
