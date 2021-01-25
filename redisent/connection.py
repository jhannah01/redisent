from __future__ import annotations

import aioredis
import redis

from redisent.utils import RedisPoolType


class RedisConnection:
    redis_pool: RedisPoolType
    is_async: bool

    @classmethod
    def build(cls, redis_url: str) -> RedisConnection:
        if not redis_url.startswith('redis://'):
            redis_url = f'redis://{redis_url}'

        redis_pool = redis.ConnectionPool.from_url(redis_url)
        return RedisConnection(redis_pool, is_async=False)

    @classmethod
    async def build_async(cls, redis_url: str) -> RedisConnection:
        """
        Build :py:mod:`asyncio` / :py:mod:`aioredis` powered instance of ``RedisConnection``

        .. note::
           This is an ``async``  method and thus must be called with ``await``

        :param redis_url: the Redis server hostname (with or without the ``redis://`` prefix)
        """
        if not redis_url.startswith('redis://'):
            redis_url = f'redis://{redis_url}'

        redis_pool = await aioredis.create_redis_pool(redis_url)
        return RedisConnection(redis_pool, is_async=True)

    def __init__(self, redis_pool: RedisPoolType, is_async: bool = None) -> None:
        self.redis_pool = redis_pool

        if is_async is None:
            is_async = isinstance(redis_pool, aioredis.ConnectionsPool)

        self.is_async = is_async

    def __repr__(self):
        return f'RedisConnection(redis_pool="{self.redis_pool}")'
