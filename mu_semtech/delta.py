from sparql import Triple


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
        return "<UpdateData graph=%s inserts=%s deletes=%s>" % (self.graph, self.inserts, self.deletes)

    def filter_inserts(self, func):
        assert callable(func)
        return (x for x in self.inserts if func(x))

    def filter_deletes(self, func):
        assert callable(func)
        return (x for x in self.deletes if func(x))
