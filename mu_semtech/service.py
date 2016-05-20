from flask import Response
from flask_restful import Resource, abort, inputs
from flask_restful.reqparse import RequestParser
import isodate

from mu_semtech.helpers import (
    ensure_post_query, escape_string, get_project, graph, get_service)


class ServiceScale(Resource):
    parser = RequestParser()
    parser.add_argument('num', type=int, required=True)
    parser.add_argument('timeout', type=int, default=10)

    @get_service
    def post(self):
        options = self.parser.parse_args()
        self.service.scale(options['num'], timeout=options['timeout'])
        ensure_post_query("""
            WITH <%(graph)s>
            DELETE {
                <http://swarmui.semte.ch/resources/services/%(uuid)s>
                swarmui:scaling
                ?scaling
            }
            INSERT {
                <http://swarmui.semte.ch/resources/services/%(uuid)s>
                swarmui:scaling
                %(new_scaling)s
            }
            WHERE {
                <http://swarmui.semte.ch/resources/services/%(uuid)s>
                swarmui:scaling
                ?scaling
            }
            """ % {
                'graph': graph,
                'uuid': self.service_id,
                'new_scaling': options['num'],
            })
        return {'status': 'ok'}


class ServiceLogs(Resource):
    parser = RequestParser()
    parser.add_argument('tail', type=inputs.positive)

    @get_service
    def get(self):
        options = self.parser.parse_args()
        containers = self.project.containers(
            service_names=[self.service.name], stopped=True)
        logs_args = {
            'follow': False,
            'tail': int(options['tail']) if options['tail'] else 'all',
            'timestamps': True,
        }
        # TODO: includes container name?
        logs = []
        for container in containers:
            lines = container.logs(stdout=True, stderr=True,
                stream=False, **logs_args).split(b'\n')
            for line in lines:
                if not line:
                    continue
                timestamp = line.split(b' ', 1)[0]
                logs.append((isodate.parse_datetime(timestamp.decode()), line))
        content = b'\n'.join(line for timestamp, line in sorted(logs))
        return Response(content, 200, mimetype='text/plain')
