from __future__ import annotations

import logging

from contextlib import contextmanager
from typing import List, Optional

import redis

from redisent.helpers import RedisError, REDIS_URL, RedisentHelper, RedisPrimitiveType

logger = logging.getLogger(__name__)


class BlockingRedisentHelper(RedisentHelper):
    @classmethod
    def build(cls, redis_url: str = REDIS_URL) -> BlockingRedisentHelper:
        pool = redis.ConnectionPool.from_url(redis_url)
        return cls(pool)

    @contextmanager
    def wrapped_redis(self, op_name: str, use_pool: redis.ConnectionPool = None):
        pool = use_pool or self.redis_pool
        try:
            r_conn = redis.Redis(connection_pool=pool)
        except Exception as ex:
            err_message = f'Unable to build new Redis connection for "{op_name}": {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex)

        try:
            yield r_conn
        except Exception as ex:
            err_message = f'Error executing Redis command "{op_name}": {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex)

    def keys(self, pattern: str = None, decode_keys: str = 'utf-8', use_pool: redis.ConnectionPool = None) -> List[str]:
        pattern = pattern or '*'
        op_name = f'keys(pattern="{pattern}")'
        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            r_keys = r_conn.keys(pattern)

        if not decode_keys:
            return r_keys

        return [r_key.decode(decode_keys) for r_key in r_keys]

    def exists(self, key: str, use_pool: redis.ConnectionPool = None) -> bool:
        op_name = f'exists(key="{key}")'
        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return (r_conn.exists(key)) == 1

    def set(self, key: str, value: RedisPrimitiveType, use_pool: redis.ConnectionPool = None) -> bool:
        op_name = f'set(key="{key}", value="...")'

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = r_conn.set(key, value)

            if not res:
                raise RedisError(f'Error setting key "{key}" in Redis: No response')

            return True

    def get(self, key: str, missing_okay: bool = False, encoding: str = None, use_pool: redis.ConnectionPool = None) -> Optional[RedisPrimitiveType]:
        op_name = f'get(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return r_conn.get(key, encoding=encoding)

    def delete(self, key: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None) -> Optional[bool]:
        op_name = f'delete(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = r_conn.delete(key)

        return res and res > 0

    # Hash-related functions
    def hkeys(self, key: str, decode_keys: str = 'utf-8', use_pool: redis.ConnectionPool = None) -> List[str]:
        op_name = f'hkeys(key="{key}")'

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return r_conn.hkeys(key, encoding=decode_keys)

    def hexists(self, key: str, name: str, use_pool: redis.ConnectionPool = None) -> bool:
        op_name = f'hexists("{key}", "{name}")'

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return r_conn.hexists(key, name)

    def hset(self, key: str, name: str, value: RedisPrimitiveType, use_pool: redis.ConnectionPool = None) -> bool:
        op_name = f'hset(key="{key}", name="{name}", value="...")'

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = r_conn.hset(key, name, value)

            if not res:
                raise RedisError(f'Error setting hash key "{key}" in Redis: No response')

            return True

    def hget(self, key: str, name: str, missing_okay: bool = False, encoding: str = None, use_pool: redis.ConnectionPool = None) -> Optional[RedisPrimitiveType]:
        op_name = f'hget(key="{key}", name="{name}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        if not self.hexists(key, name, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry')

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return r_conn.hget(key, name, encoding=encoding)
