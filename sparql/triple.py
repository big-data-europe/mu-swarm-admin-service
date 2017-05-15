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
        return "<%s type=%s value=%s datatype=%s>" % \
            (self.__class__.__name__, self.type, self.value, self.datatype)

    def __hash__(self):
        return hash((self.type, self.value))


class Triple:
    def __init__(self, data):
        assert isinstance(data, dict)
        assert "s" in data
        assert isinstance(data['s'], dict)
        assert "p" in data
        assert isinstance(data['p'], dict)
        assert "o" in data
        assert isinstance(data['o'], dict)
        self.s = Value(data['s'])
        self.p = Value(data['p'])
        self.o = Value(data['o'])

    def __repr__(self):
        return "<%s s=%s p=%s o=%s>" % (self.__class__.__name__, self.s, self.p, self.o)

    def __hash__(self):
        return hash((hash(self.s), hash(self.p), hash(self.o)))

    def __eq__(self, other):
        if isinstance(other, Triple):
            return hash(self) == hash(other)
        else:
            return False
