class MockCache(object):
    """
    Simple cache to use for mocking Memcached
    """
    def __init__(self):
        super(MockCache, self).__init__()
        self._cache = {}

    def set(self, k, v):
        self._cache[k] = v
        return True

    def get(self, k, default=None):
        return self._cache.get(k, default)

    def delete(self, k):
        if k in self._cache:
            del self._cache[k]
        return True

    def set_many(self, items):
        for k, v in items.iteritems():
            self.set(k, v)
        return True

    def delete_many(self, items):
        for k in items:
            self.delete(k)
        return True
