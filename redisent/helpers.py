from __future__ import annotations

import asyncio

import aioredis
import logging
import pickle
import redis
import functools

from contextlib import contextmanager, asynccontextmanager
from typing import Callable

from redisent.errors import RedisError
from redisent.utils import RedisPoolType

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType
    use_async: bool = False

    @classmethod
    def build(cls, redis_pool: redis.ConnectionPool = None, redis_url: str = None) -> RedisentHelper:
        """
        Builder class method for creating a blocking ``redis``-powered helper instance

        This method will handle building the pool, if not provided, from the value of ``redis_url`` which will be formatted by
        the :py:meth:`RedisentHelper.format_redis_url` method.

        :param redis_pool: if provided, this :py:class:`redis.ConnectionPool` instance will be used for the Redis pool
        :param redis_url: if no value for ``redis_pool`` is provided, use this value to build a Redis URL and connection pool
        """

        if not redis_pool:
            if not redis_url:
                raise ValueError('No value provided for "redis_pool" or "redis_url"')

            redis_url = cls.format_redis_url(redis_url)
            redis_pool = redis.ConnectionPool.from_url(redis_url)

        return RedisentHelper(redis_pool=redis_pool, use_async=False)

    @classmethod
    async def build_async(cls, redis_pool: aioredis.ConnectionsPool = None, redis_url: str = None) -> RedisentHelper:
        """
        Builder class method for creating an ``aioredis``-powered helper instance

        This method will handle building the pool, if not provided, from the value of ``redis_url`` which will be formatted by
        the :py:meth:`RedisentHelper.format_redis_url` method.

        :param redis_pool: if provided, this :py:class:`aioredis.ConnectionsPool` instance will be used for the Redis pool
        :param redis_url: if no value for ``redis_pool`` is provided, use this value to build a Redis URL and connection pool
        """

        if not redis_pool:
            if not redis_url:
                raise ValueError('No value provided for "redis_pool" or "redis_url"')

            redis_url = cls.format_redis_url(redis_url)
            redis_pool = await aioredis.create_redis_pool(redis_url)

        return RedisentHelper(redis_pool=redis_pool, use_async=True)

    @classmethod
    def format_redis_url(cls, redis_url: str) -> str:
        """
        Helper class method for correctly formatting a given hostname into a ``redis://``-prefixed URI

        :param redis_url: the hostname with or without the ``redis://`` prefix
        """

        return f'redis://{redis_url}' if not redis_url.startswith('redis://') else redis_url

    def __init__(self, redis_pool: RedisPoolType, use_async: bool = None) -> None:
        """
        Simple ``ctor`` method called by both the :py:meth:`RedisentHelper.build` and :py:meth:`RedisentHelper.build_async` methods

        :param redis_pool:  the Redis pool to use. If ``use_async`` is set to ``True`` this should be a configured instance
                            of :py:class:`aioredis.ConnectionsPool`, otherwise this should be an instance of :py:class:`redis.ConnectionPool`

        :param use_async:   indicates if ``aioredis`` should be used under the hood. if not provided, the type of the value
                            for the``redis_pool`` argument is interrogated to determine if this instance should use ``asyncio`` methods or not
        """
        self.redis_pool = redis_pool

        if use_async is None:
            use_async = isinstance(redis_pool, aioredis.ConnectionsPool)

        self.use_async = use_async

    @staticmethod
    def _handle_decode_attempt(res, use_encoding: str = None, decode_handler: Callable = None):
        """
        Internal handler for attempting to intelligently decode any discovered :py:class:`redisent.models.RedisEntry` instances found

        :param use_encoding: if provided, indicates the results should be decoded using the provided encoding (generally ``utf-8``)
        :param first_handler: optional callback that will be called when attempting to decode response
        """

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

    def decode_entries(self, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        """
        Decorator used for automatically attempting to decode the returned value of a method using the static method ``_handle_decode_attempt``

        This is helpful for automatically returning :py:class:`redisent.models.RedisEntry` instances and / or the opportunity to interact with the results via the
        two ``Callable`` arguments ``first_handler`` and ``final_handler``.

        If provided, the ``first_handler`` is used first, prior to attempting to using the ``_handle_decode_attempt`` static method. Finally, if provided,
        the ``final_handler`` will be called prior to passing the possibly decoded response back to the caller.

        This decorator can be used with ``asyncio`` coroutine or regular methods. The inner decorator uses :py:func:`asyncio.iscoroutinefunction` to determine if
        the wrapped method is a a coroutine and calls the handlers accordingly.

        :param use_encoding: if provided, indicates the results should be decoded using the provided encoding (generally ``utf-8``)
        :param first_handler: first callback handler to invoke __prior__ to attempting to decode the result
        :param final_handler: final callback handler to invoke __after__ attempting to decode the result
        """

        def _outer_wrapper(func):
            if asyncio.iscoroutinefunction(func):
                @functools.wraps(func)
                async def _async_wrapper(*args, **kwargs):
                    res = await func(*args, **kwargs)
                    return first_handler(res) if first_handler else self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _async_wrapper
            else:
                @functools.wraps(func)
                def _blocking_wrapper(*args, **kwargs):
                    res = func(*args, **kwargs)
                    return first_handler(res) if first_handler else self._handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _blocking_wrapper

        return _outer_wrapper

    wrapped_redis = property(fget=lambda self: self._wrapped_redis_blocking if not self.use_async else self._wrapped_redis_async)

    @contextmanager
    def _wrapped_redis_blocking(self, op_name: str, use_pool: redis.ConnectionPool = None):
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
    async def _wrapped_redis_async(self, op_name: str, use_pool: aioredis.ConnectionsPool = None):
        pool = use_pool or self.redis_pool

        try:
            logger.debug(f'Executing Async Redis command for "{op_name}"...')
            yield pool
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{op_name}": {ex}')
            raise RedisError(f'Redis Error executing "{op_name}": {ex}', base_exception=ex, related_command=op_name)
