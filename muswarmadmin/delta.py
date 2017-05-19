from aiohttp import web
from aiosparql.syntax import IRI, Literal
from itertools import groupby

from muswarmadmin import pipelines, repositories, services


class Value:
    def __init__(self, data):
        assert isinstance(data, dict)
        assert "type" in data
        assert isinstance(data['type'], str)
        assert "value" in data
        assert isinstance(data['value'], str)
        self.type = data['type']
        self.value = data['value']
        self.datatype = data.get('datatype')

    def __eq__(self, other):
        if isinstance(other, Value):
            return (
                self.type is other.type and self.value is other.value
            )
        else:
            return self.value == other

    def __repr__(self):
        return "<%s type=%s value=%s>" % \
            (self.__class__.__name__, self.type, self.value)

    def __hash__(self):
        return hash((self.type, self.value))


class Triple:
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
        assert callable(func)
        return (x for x in self.inserts if func(x))

    def filter_deletes(self, func):
        assert callable(func)
        return (x for x in self.deletes if func(x))


def filter_objects(data, resource_type):
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

    await repositories.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "repositories/"))

    await pipelines.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "pipeline-instances/"))

    await services.update(
        request.app,
        *filter_objects(
            first_data,
            request.app.base_resource + "services/"))

    raise web.HTTPNoContent()
