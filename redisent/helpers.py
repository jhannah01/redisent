from __future__ import annotations

import asyncio

import aioredis
import logging
import pickle
import redis
import functools

from contextlib import contextmanager, asynccontextmanager
from typing import List, Optional, Union, Mapping, Any, Callable
from pprint import pformat

from redisent.errors import RedisError
from redisent.utils import RedisPoolType, REDIS_URL, RedisPrimitiveType

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType

    use_async: bool = False

    def __init__(self, redis_pool: RedisPoolType, use_async: bool = False) -> None:
        self.redis_pool = redis_pool
        self.use_async = use_async

    def __del__(self):
        if self.use_async:
            self.redis_pool.close()

    def decode_entries(self, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        def _outer_wrapper(func):
            @functools.wraps(func)
            def _blocking_wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                if first_handler:
                    return first_handler(res)

                return self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

            return _blocking_wrapper

        return _outer_wrapper

    def decode_entries_async(self, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        def _outer_wrapper(func):
            @functools.wraps(func)
            async def _inner_wrapper(*args, **kwargs):
                res = await func(*args, **kwargs)

                if first_handler:
                    return first_handler(res)

                return self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

            return _inner_wrapper

        return _outer_wrapper

    @staticmethod
    def _handle_decode_attempt(res, use_encoding: str = None, decode_handler: Callable = None):
        if not res:
            return res

        def decode_value(value):
            try:
                return pickle.loads(value)
            except pickle.PickleError:
                if decode_handler:
                    return decode_handler(value)
                elif use_encoding:
                    return value.decode(use_encoding)

                return value

        if isinstance(res, list):
            res = [decode_value(ent) for ent in res]
        elif isinstance(res, dict):
            res = {ent_name.decode(use_encoding) if use_encoding else ent_name: decode_value(ent_value) for ent_name, ent_value in res.items()}
        elif use_encoding:
            res = res.decode(use_encoding)

        return res

    @classmethod
    def build(cls, redis_pool: Union[RedisPoolType, str], use_async: bool = False) -> RedisentHelper:
        if isinstance(redis_pool, str):
            if use_async:
                loop = asyncio.get_event_loop()
                redis_pool = loop.run_until_complete(cls.build_async_pool(redis_pool))
            else:
                redis_pool = cls.build_blocking_pool(redis_pool)

        return cls(redis_pool, use_async)

    @classmethod
    def build_blocking_pool(cls, redis_url: str = REDIS_URL) -> redis.ConnectionPool:
        return redis.ConnectionPool.from_url(redis_url)

    @classmethod
    async def build_async_pool(cls, redis_url: str = REDIS_URL) -> aioredis.ConnectionsPool:
        redis_url = redis_url if redis_url.startswith('redis://') else f'redis://{redis_url}'
        return await aioredis.create_redis_pool(address=redis_url)

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
            raise RedisError(err_message, base_exception=ex, related_command=op_name)

    @asynccontextmanager
    async def wrapped_redis_async(self, op_name: str, use_pool: aioredis.ConnectionsPool = None):
        pool = use_pool or self.redis_pool

        try:
            logger.debug(f'Executing Async Redis command for "{op_name}"...')
            yield pool
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{op_name}": {ex}')
            raise RedisError(f'Redis Error executing "{op_name}": {ex}', base_exception=ex, related_command=op_name)
