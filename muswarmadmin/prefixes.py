from aiosparql.syntax import IRI, Namespace, PrefixedName

__all__ = """
    SwarmUI Mu Ext Dct Doap W3Vocab Foaf Auth Session
    """.split()


class SwarmUI(Namespace):
    __iri__ = IRI("http://swarmui.semte.ch/vocabularies/core/")

    Down = PrefixedName
    Restarting = PrefixedName
    Scaling = PrefixedName
    Service = PrefixedName
    Starting = PrefixedName
    Stopped = PrefixedName
    Stopping = PrefixedName
    Up = PrefixedName
    pipelines = PrefixedName
    requestedStatus = PrefixedName
    restartRequested = PrefixedName
    scaling = PrefixedName
    services = PrefixedName


class Mu(Namespace):
    __iri__ = IRI("http://mu.semte.ch/vocabularies/core/")

    uuid = PrefixedName


class Ext(Namespace):
    __iri__ = IRI("http://mu.semte.ch/vocabularies/ext/")


class Dct(Namespace):
    __iri__ = IRI("http://purl.org/dc/terms/")

    title = PrefixedName


class Doap(Namespace):
    __iri__ = IRI("http://usefulinc.com/ns/doap#")

    location = PrefixedName


class Foaf(Namespace):
    __iri__ = IRI("http://xmlns.com/foaf/0.1/")


class Auth(Namespace):
    __iri__ = IRI("http://mu.semte.ch/vocabularies/authorization/")


class Session(Namespace):
    __iri__ = IRI("http://mu.semte.ch/vocabularies/session/")


class W3Vocab(Namespace):
    __iri__ = IRI("https://www.w3.org/1999/xhtml/vocab#")