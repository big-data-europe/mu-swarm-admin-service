class Triple:
    def __init__(self, data):
        assert isinstance(data, dict)
        assert "s" in data
        assert isinstance(data['s'], str)
        assert "p" in data
        assert isinstance(data['p'], str)
        assert "o" in data
        assert isinstance(data['o'], str)
        self.s = data['s']
        self.p = data['p']
        self.o = data['o']

    def __repr__(self):
        return "<%s s=%s p=%s o=%s>" % (self.__class__.__name__, self.s, self.p, self.o)
