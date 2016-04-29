from flask import Response
from flask_restful import Resource, abort, inputs
from flask_restful.reqparse import RequestParser
from flask_restful_sparql.http import Client
from flask_restful_sparql.escaping import escape_string
import logging
from os import environ as ENV

from mu_semtech.helpers import get_project, get_service


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


class ServiceScale(Resource):
    parser = RequestParser()
    parser.add_argument('num', type=int, required=True)
    parser.add_argument('timeout', type=int, default=10)

    @get_project
    @get_service
    def post(self):
        options = self.parser.parse_args()
        self.service.scale(options['num'], timeout=options['timeout'])
        return {'status': 'ok'}


class ServiceLogs(Resource):
    parser = RequestParser()
    parser.add_argument('follow', type=inputs.boolean)
    parser.add_argument('timestamps', type=inputs.boolean)
    parser.add_argument('tail', type=inputs.positive)

    @get_project
    @get_service
    def get(self):
        options = self.parser.parse_args()
        containers = self.project.containers(
            service_names=[self.service.name], stopped=True)
        logs_args = {
            'follow': options['follow'],
            'tail': int(options['tail']) if options['tail'] else 'all',
            'timestamps': options['timestamps']
        }
        # TODO: includes container name?
        if not options['follow']:
            content = b""
            for container in containers:
                content += container.logs(stdout=True, stderr=True,
                    stream=False, **logs_args)
            return Response(content, 200, mimetype='text/plain')
        else:
            abort(501,
                title="the feature has not been implemented",
                detail="...but there is hope")
