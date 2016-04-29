from flask import request, abort


def check_pipeline_permissions():
    print(request.path)
