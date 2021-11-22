from cacheout import Cache

# global cache object:
_cache = Cache()


def get(key, default=None):
    global _cache
    return _cache.get(key, default)


def set(key, value, ttl=None):
    global _cache
    _cache.set(key, value, ttl)


def delete(key):
    global _cache
    _cache.delete(key)


