from sparql import Triple


class DeltaInsert(Triple):
    pass

class DeltaDelete(Triple):
    pass

class UpdateData:
    def __init__(self, data):
        assert isinstance(data['graph'], str)
        assert isinstance(data['inserts'], list)
        assert isinstance(data['deletes'], list)
        self.graph = data['graph']
        self.inserts = list(map(DeltaInsert, data['inserts']))
        self.deletes = list(map(DeltaDelete, data['deletes']))

    def __repr__(self):
        return "<UpdateData graph=%s inserts=%s deletes=%s>" % (self.graph, self.inserts, self.deletes)

    def filter_inserts(self, func):
        assert callable(func)
        return (x for x in self.inserts if func(x))
