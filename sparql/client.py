import logging
import requests
from urllib.parse import quote_plus

from .constants import endpoint_url
from .prefix import Prefix
from . import prefixes

__all__ = ['SPARQLRequestFailed', 'Client', 'client']


class SPARQLRequestFailed(Exception):
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body


class Client:
    logger = logging.getLogger(__name__)

    def __init__(self, base_url, session=None, prefixes=None):
        if session is None:
            session = requests.Session()
        self.base_url = base_url
        self.prefixes = prefixes
        self.session = session

    def _make_query(self, query):
        if self.prefixes == None:
            return query
        else:
            lines = [
                "PREFIX %s: <%s>" % (x.label, x.base_uri)
                for x in vars(self.prefixes).values() if isinstance(x, Prefix)
            ]
            lines.extend(["", query])
            return "\n".join(lines)

    def query(self, query, headers=None):
        if headers is None:
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        complete_query = self._make_query(query)
        self.logger.debug("Sending GET query to %s:\n%s", self.base_url, complete_query)
        response = self.session.post(
            self.base_url, data={'query': complete_query},
            headers=headers)
        return response

    def update(self, query, headers=None):
        if headers is None:
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        complete_query = self._make_query(query)
        self.logger.debug("Sending POST query to %s:\n%s", self.base_url, complete_query)
        response = self.session.post(
            self.base_url, data={'update': complete_query},
            headers=headers)
        return response

    def ensure_query(self, query, headers=None):
        resp = self.query(query, headers=headers)
        if resp.status_code >= 300:
            raise SPARQLRequestFailed(resp.status_code, resp.text)
        return resp.json()

    def ensure_update(self, query, headers=None):
        resp = self.update(query, headers=headers)
        if resp.status_code >= 300:
            raise SPARQLRequestFailed(resp.status_code, resp.text)
        return resp.json()


client = Client(endpoint_url, prefixes=prefixes)
