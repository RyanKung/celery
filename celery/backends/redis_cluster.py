# -*- coding: utf-8 -*-
"""Redis cluster result store backend."""

from celery.backends.redis import RedisBackend
from celery.exceptions import ImproperlyConfigured

try:
    from rediscluster.client import RedisCluster
    from rediscluster.exceptions import RedisClusterError
    from kombu.transport.redis import get_redis_error_classes

except ImportError:
    RedisCluster = None
    RedisClusterError = None
    get_redis_error_classes = None

REDIS_CLUSTER_MISSING = """\
You need to install the redis cluster library in order to use \
the Redis result store backend."""


class RedisClusterBackend(RedisBackend):
    def __init__(self, host=None, port=None, db=None, password=None,
                 max_connections=None, url=None,
                 connection_pool=None, **kwargs):
        super(RedisBackend, self).__init__(expires_type=int, **kwargs)
        if self.redis is None:
            raise ImproperlyConfigured(REDIS_CLUSTER_MISSING)

        _get = self.app.conf.get
        self.max_connections = (
            max_connections or
            _get('redis_max_connections') or
            self.max_connections)
        self._ConnectionPool = connection_pool

        self.connparams = self.app.conf.get('CELERY_REDIS_CLUSTER_SETTINGS', {
            'startup_nodes': [{'host': _get('HOST') or 'localhost', 'port': _get('PORT') or 6379}]
        })
        if self.connparams is not None:
            if not isinstance(self.connparams, dict):
                raise ImproperlyConfigured(
                    'RedisCluster backend settings should be grouped in a dict')
        self.expires = self.prepare_expires(None, type=int)
        self.connection_errors, self.channel_errors = (
            get_redis_error_classes() if get_redis_error_classes
            else ((), ()))
