import logging
import requests
from urllib.parse import quote_plus


class Client(object):
    """
    A client represents a way to connect to a SPARQL endpoint
    over http. 
    """

    logger = logging.getLogger(__name__)

    def __init__(self, base_url, session=None):
        """
        Initializes the client object with a session and the url of the
        SPARQL endpoint. TODO: check the validity of the endpoint?
        
        @param base_url the url of the end point
        @param session optional the current session
        
        @result the client is initialized with a [valid] endpoint and a session
        """
        if session is None:
            session = requests.Session()
        self.base_url = base_url
        self.session = session

    def get_query(self, query, headers=None):
        """
        Perform a get query on the endpoint with the passed headers. If no 
        Accept header was set we will set it to JSON.
        
        @param the query that has to be evaluated
        @param headers optional the headers that need to be used for the query

        @pre the query should be a valid GET query

        @return the response from the endpoint on the given query/headers
        """
        if headers is None:
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        self.logger.debug("Sending GET query to %s:\n%s", self.base_url, query)
        response = self.session.get(
            "{}?query={}".format(self.base_url, quote_plus(query)),
            headers=headers)
        return response

    def post_query(self, query, headers=None):
        """
        Perform a post query on the endpoint with the past headers. If no Accept
        headers was set we will set it to JSON.

        @param query the query to be evaluated
        @param headers optional the headers that should be used

        @pre the query should be a valid POST query

        @return the response from the endpoint
        """
        if headers is None:
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        self.logger.debug("Sending POST query to %s:\n%s", self.base_url, query)
        response = self.session.post(
            self.base_url, data={'query': query}, headers=headers)
        return response
