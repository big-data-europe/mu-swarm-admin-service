from os import environ as ENV

__all__ = ['graph']


endpoint_url = ENV.get('MU_SPARQL_ENDPOINT', 'http://database:8890/sparql')
graph = ENV.get('MU_APPLICATION_GRAPH', 'http://mu.semte.ch/application')
