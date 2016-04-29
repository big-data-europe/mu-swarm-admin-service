#!/usr/bin/env python3

import flask
import os

from mu_semtech.api import Api
from mu_semtech.pipeline import *
from mu_semtech.service import *
from mu_semtech.permissions import check_pipeline_permissions


app = flask.Flask('mu-swarm-admin')
api = Api(app)

api.add_resource(PipelineList, '/pipelines')
api.add_resource(PipelineUp, '/pipelines/<project_id>/up')
api.add_resource(PipelineDown, '/pipelines/<project_id>/down')
api.add_resource(PipelineStop, '/pipelines/<project_id>/stop')
api.add_resource(ServiceScale, '/pipelines/<project_id>/services/<service_name>/scale')
api.add_resource(ServiceLogs, '/pipelines/<project_id>/services/<service_name>/logs')

app.before_request(check_pipeline_permissions)

app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '5000')),
    debug=(os.environ.get('ENV', 'prod') == 'dev'))
