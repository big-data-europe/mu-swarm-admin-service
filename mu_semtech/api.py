from flask import current_app
import flask_restful
from werkzeug.datastructures import Headers
from werkzeug.exceptions import HTTPException


__all__ = ['Api']


class Api(flask_restful.Api):
    def handle_error(self, exc):
        try:
            data = {
                'errors': [],
            }
            if isinstance(exc, HTTPException):
                code = exc.code
                headers = exc.get_response().headers
                if not hasattr(exc, 'data'):
                    raise
                data['errors'].append(exc.data)
            else:
                code = 500
                headers = Headers()
                data['errors'].append({
                    'title', 'Internal Server Error',
                })
            headers.pop('Content-Length', None)
            resp = self.make_response(data, code, headers)
            return resp
        except Exception:
            current_app.logger.exception("Exception during the error handling")
            raise
