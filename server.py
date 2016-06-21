#!/usr/bin/env python3

import flask
import os

from mu_semtech.api import Api
from mu_semtech.pipeline import *
from mu_semtech.service import *


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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '80')),
        debug=(os.environ.get('ENV', 'prod').startswith('dev')))
