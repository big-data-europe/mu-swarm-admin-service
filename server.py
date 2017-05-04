#!/usr/bin/env python3

import flask
from itertools import groupby
import logging
import os

from mu_semtech.api import Api
from mu_semtech.delta import UpdateData
from mu_semtech.pipeline import *
from mu_semtech.service import *
from sparql import client, graph


app = flask.Flask('mu-swarm-admin')
api = Api(app)

api.add_resource(PipelineList, '/repositories/<repository_id>/pipelines')
api.add_resource(PipelineUp, '/pipelines/<project_id>/up')
api.add_resource(PipelineDown, '/pipelines/<project_id>/down')
api.add_resource(PipelineStop, '/pipelines/<project_id>/stop')
api.add_resource(PipelineRestart, '/pipelines/<project_id>/restart')
api.add_resource(ServiceScale, '/services/<service_id>/scale')
api.add_resource(ServiceLogs, '/services/<service_id>/logs')
api.add_resource(ServiceRestart, '/services/<service_id>/restart')
api.add_resource(ServiceInspect, '/services/<service_id>/inspect')


@app.route("/update", methods=['POST'])
def receive_update():
    data = [UpdateData(x) for x in flask.request.get_json()['delta']]
    app.logger.debug("Received: %r", data)
    try:
        my_data = next(x for x in data if x.graph == graph)
    except StopIteration:
        return ("", 204)
    pipelines = {
        s: list(group)
        for s, group in groupby(my_data.filter_inserts(
            lambda x: x.s.value.startswith("http://swarm-ui.big-data-europe.eu/resources/pipeline-instances/")),
            lambda x: x.s.value)
    }
    update_pipelines(pipelines)
    services = {
        s: list(group)
        for s, group in groupby(my_data.filter_inserts(
            lambda x: x.s.value.startswith("http://swarm-ui.big-data-europe.eu/resources/services/")),
            lambda x: x.s.value)
    }
    update_services(services)
    repositories = {
        s: list(group)
        for s, group in groupby(my_data.filter_inserts(
            lambda x: x.s.value.startswith("http://swarm-ui.big-data-europe.eu/resources/repositories/")),
            lambda x: x.s.value)
    }
    update_repositories(repositories)
    return ("", 204)

if __name__ == '__main__':
    debug = os.environ.get('ENV', 'prod').startswith('dev')
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '80')),
        debug=debug)
