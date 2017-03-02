class Prefix:
    def __init__(self, label, base_uri):
        assert isinstance(label, str)
        assert isinstance(base_uri, str)
        self.label = label
        self.base_uri = base_uri

    def __repr__(self):
        return "<%s %s %s>" % (type(self).__name__, self.label, self.base_uri)

    def get(self, suffix):
        assert isinstance(suffix, str)
        return self.base_uri + suffix

    def __add__(self, suffix):
        return self.get(suffix)


rdf = Prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
