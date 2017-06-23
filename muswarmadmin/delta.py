from aiohttp import web
from aiosparql.syntax import IRI, Literal
from itertools import groupby

from muswarmadmin import pipelines, repositories, services


class Triple:
    """
    A triple: subject (s), predicate (p) and object (o)
    """
    def __init__(self, data):
        assert isinstance(data, dict)
        assert "s" in data
        assert isinstance(data['s'], dict) and data['p']['type'] == "uri"
        assert "p" in data
        assert isinstance(data['p'], dict) and data['p']['type'] == "uri"
        assert "o" in data
        assert isinstance(data['o'], dict)
        self.s = IRI(data['s']['value'])
        self.p = IRI(data['p']['value'])
        if data['o']['type'] == "uri":
            self.o = IRI(data['o']['value'])
        elif data['o']['type'] == "literal":
            self.o = Literal(data['o']['value'])
        else:
            raise NotImplementedError("object type %s" % data['o']['type'])

    def __repr__(self):
        return "<%s s=%s p=%s o=%s>" % (
            self.__class__.__name__, self.s, self.p, self.o)

    def __hash__(self):
        return hash((hash(self.s), hash(self.p), hash(self.o)))

    def __eq__(self, other):
        if isinstance(other, Triple):
            return hash(self) == hash(other)
        else:
            return False


class UpdateData:
    """
    A Delta service update: all its inserts, all its deletes
    """
    def __init__(self, data):
        assert isinstance(data['graph'], str)
        assert isinstance(data['inserts'], list)
        assert isinstance(data['deletes'], list)
        self.graph = data['graph']
        inserts = set(map(Triple, data['inserts']))
        deletes = set(map(Triple, data['deletes']))
        null_operations = inserts & deletes
        self.inserts = list(inserts - null_operations)
        self.deletes = list(deletes - null_operations)

    def __repr__(self):
        return "<%s graph=%s inserts=%s deletes=%s>" % (
            self.__class__.__name__, self.graph, self.inserts, self.deletes)

    def filter_inserts(self, func):
        """
        Filter inserts that func(x) match where x is a singe triple of the
        update
        """
        assert callable(func)
        return (x for x in self.inserts if func(x))

    def filter_deletes(self, func):
        """
        Filter deletes that func(x) match where x is a singe triple of the
        update
        """
        assert callable(func)
        return (x for x in self.deletes if func(x))


def filter_objects(data, resource_type):
    """
    Filter updates for a resource type
    """
    inserts = {
        s: list(group)
        for s, group in groupby(data.filter_inserts(
            lambda x: x.s.value.startswith(resource_type.value)),
            lambda x: x.s)
    }
    deletes = {
        s: list(group)
        for s, group in groupby(data.filter_deletes(
            lambda x: x.s.value.startswith(resource_type.value)),
            lambda x: x.s)
    }
    return (inserts, deletes)


async def update(request):
    """
    The API entry point for the Delta service callback
    """
    graph = request.app.sparql.graph
    try:
        data = await request.json()
    except:
        raise web.HTTPBadRequest(body="invalid json")
    try:
        data = [UpdateData(x) for x in data['delta']]
    except:
        request.app.logger.exception("Cannot parse delta payload")
        raise web.HTTPBadRequest(body="cannot parse deltas received")
    try:
        first_data = next(x for x in data if x.graph == graph)
    except StopIteration:
        raise web.HTTPNoContent()

    request.app.loop.create_task(repositories.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "repositories/")))

    request.app.loop.create_task(pipelines.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "pipeline-instances/")))

    request.app.loop.create_task(services.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "services/")))

    raise web.HTTPNoContent()
