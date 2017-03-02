from .prefix import Prefix, rdf

__all__ = """
    rdf swarmui mu ext dct doap w3vocab foaf auth session
    """.split()


swarmui = Prefix("swarmui", "http://swarmui.semte.ch/vocabularies/core/")
mu = Prefix("mu", "http://mu.semte.ch/vocabularies/core/")
ext = Prefix("ext", "http://mu.semte.ch/vocabularies/ext/")
dct = Prefix("dct", "http://purl.org/dc/terms/")
doap = Prefix("doap", "http://usefulinc.com/ns/doap#")
w3vocab = Prefix("w3vocab", "https://www.w3.org/1999/xhtml/vocab#")
foaf = Prefix("foaf", "http://xmlns.com/foaf/0.1/")
auth = Prefix("auth", "http://mu.semte.ch/vocabularies/authorization/")
session = Prefix("session", "http://mu.semte.ch/vocabularies/session/")
