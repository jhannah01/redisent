from __future__ import annotations

import asyncio

import aioredis
import logging
import pickle
import redis
import functools

from contextlib import contextmanager, asynccontextmanager
from typing import Union, Callable, Optional

from redisent.errors import RedisError
from redisent.utils import RedisPoolType, REDIS_URL

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType

    use_async: bool = False
    _loop: asyncio.AbstractEventLoop

    def __init__(self, redis_pool: RedisPoolType = None, use_async: bool = False, redis_url: str = None) -> None:
        redis_url = redis_url or REDIS_URL
        redis_url = f'redis://{redis_url}' if not redis_url.startswith('redis://') else redis_url
        self.use_async = use_async

        if not use_async:
            self.redis_pool = redis_pool or redis.ConnectionPool.from_url(redis_url)
            return

        self._loop = self.get_event_loop()
        if not redis_pool:
            redis_pool = self._loop.run_until_complete(aioredis.create_redis_pool(redis_url))

        self.redis_pool = redis_pool

    def __del__(self):
        if self.use_async:
            logger.debug('Cleaning up async Redis pool')
            self.redis_pool.close()
            self.async_loop.run_until_complete(self.redis_pool.wait_closed())

    @staticmethod
    def get_event_loop() -> asyncio.AbstractEventLoop:
        _loop = asyncio.get_event_loop()

        if _loop:
            logger.debug(f'Creating new event loop (cannot find running one)')
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)

        return _loop

    @property
    def async_loop(self) -> asyncio.AbstractEventLoop:
        if not self._loop:
            self._loop = self.get_event_loop()

        return self._loop

    def cleanup(self) -> bool:
        if not self.use_async or not isinstance(self.redis_pool, aioredis.ConnectionsPool):
            return False

        self.redis_pool.close()
        self._loop.run_until_complete(self.redis_pool.wait_closed())
        return True

    def decode_entries(self, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        def _outer_wrapper(func):
            if asyncio.iscoroutinefunction(func):
                @functools.wraps(func)
                async def _async_wrapper(*args, **kwargs):
                    res = await func(*args, **kwargs)
                    return first_handler(res) if first_handler else self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _async_wrapper
            else:
                @functools.wraps(func)
                def _blocking_wraper(*args, **kwargs):
                    res = func(*args, **kwargs)
                    return first_handler(res) if first_handler else self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _blocking_wraper

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

    wrapped_redis = property(fget=lambda self: self.wrapped_redis_blocking if not self.use_async else self.wrapped_redis_async)

    @contextmanager
    def wrapped_redis_blocking(self, op_name: str, use_pool: redis.ConnectionPool = None):
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
