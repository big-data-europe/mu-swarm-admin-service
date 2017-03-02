from flask import Response
from flask_restful import abort, inputs
from flask_restful.reqparse import RequestParser
import isodate

from mu_semtech.helpers import (
    get_resource_id, get_resource_title, get_service, get_service_pipeline,
    open_project)
from mu_semtech.pipeline import BasePipelineResource
from sparql import client, graph
from sparql.prefixes import swarmui


def update_services(services):
    for subject, triples in services.items():
        for triple in triples:
            if triple.p == swarmui.get("scaling"):
                pipeline_iri = get_service_pipeline(triple.s)
                service_name = get_resource_title(subject)
                project_id = get_resource_id(pipeline_iri)
                project = open_project(project_id)
                service = project.get_service(service_name)
                service.scale(int(triple.o))


class ServiceScale(BasePipelineResource):
    parser = RequestParser()
    parser.add_argument('num', type=int, required=True)
    parser.add_argument('timeout', type=int, default=10)

    @get_service
    def post(self):
        self.check_permissions(self.project.name)
        options = self.parser.parse_args()
        self.service.scale(options['num'], timeout=options['timeout'])
        client.ensure_update("""
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


class ServiceLogs(BasePipelineResource):
    parser = RequestParser()
    parser.add_argument('tail', type=inputs.positive)

    @get_service
    def get(self):
        self.check_permissions(self.project.name)
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


class ServiceRestart(BasePipelineResource):
    @get_service
    def post(self):
        self.check_permissions(self.project.name)
        self.project.restart(service_names=[self.service.name])
        return {'status': 'ok'}


class ServiceInspect(BasePipelineResource):
    parser = RequestParser()
    parser.add_argument('index', type=inputs.positive)

    @get_service
    def get(self):
        self.check_permissions(self.project.name)
        options = self.parser.parse_args()
        if not options['index']:
            containers = self.project.containers(
                service_names=[self.service.name], stopped=True)
            return [
                container.inspect()
                for container in containers
            ]
        else:
            container = self.service.get_container(number=options['index'])
            return container.inspect()
