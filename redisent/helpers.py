from __future__ import annotations

import asyncio
import sys

import aioredis
import logging
import pickle

import redis
import functools

from contextlib import contextmanager, asynccontextmanager
from typing import Callable, Union, Any, Optional

from redisent.errors import RedisError
from redisent.utils import RedisPoolType

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType
    is_async: bool

    @staticmethod
    def handle_decode_attempt(result_value, use_encoding: str = None, decode_handler: Callable[[Any], Optional[Any]] = None):
        """
        Internal handler for attempting to intelligently decode any discovered :py:class:`redisent.models.RedisEntry` instances found

        :param result_value: the result to attempt to decode
        :param use_encoding: if provided, indicates the results should be decoded using the provided encoding (generally ``utf-8``)
        :param decode_handler: optional callback that will be called when attempting to decode response
        """

        if not result_value:
            return result_value

        def decode_value(value):
            try:
                return pickle.loads(value)
            except pickle.PickleError:
                if decode_handler:
                    return decode_handler(value)
                elif use_encoding:
                    return value.decode(use_encoding)

                return value

        if isinstance(result_value, list):
            result_value = [decode_value(ent) for ent in result_value]
        elif isinstance(result_value, dict):
            result_value = {ent_name.decode(use_encoding) if use_encoding else ent_name: decode_value(ent_value) for ent_name, ent_value in result_value.items()}
        elif use_encoding:
            result_value = result_value.decode(use_encoding)

        return result_value

    def decode_entries(self, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        """
        Decorator used for automatically attempting to decode the returned value of a method using the static method ``handle_decode_attempt``

        This is helpful for automatically returning :py:class:`redisent.models.RedisEntry` instances and / or the opportunity to interact with
        the results via the two ``Callable`` arguments ``first_handler`` and ``final_handler``.

        If provided, the ``first_handler`` is used first, prior to attempting to using the ``handle_decode_attempt`` static method. Finally,
        if provided, the ``final_handler`` will be called prior to passing the possibly decoded response back to the caller.

        This decorator can be used with ``asyncio`` coroutine or regular methods. The inner decorator uses :py:func:`asyncio.iscoroutinefunction`
        to determine if the wrapped method is a a coroutine and calls the handlers accordingly.

        :param use_encoding: if provided, indicates the results should be decoded using the provided encoding (generally ``utf-8``)
        :param first_handler: first callback handler to invoke __prior__ to attempting to decode the result
        :param final_handler: final callback handler to invoke __after__ attempting to decode the result
        """

        def _outer_wrapper(func):
            if asyncio.iscoroutinefunction(func):
                @functools.wraps(func)
                async def _async_wrapper(*args, **kwargs):
                    res = await func(*args, **kwargs)
                    return first_handler(res) if first_handler else self.handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _async_wrapper
            else:
                @functools.wraps(func)
                def _blocking_wrapper(*args, **kwargs):
                    res = func(*args, **kwargs)
                    return first_handler(res) if first_handler else self.handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

                return _blocking_wrapper

        return _outer_wrapper

    async def get_connection_async(self) -> aioredis.Redis:
        """
        Asynchronous method for creating a new Redis connection which will return a connection :py:class:`aioredis.Redis`

        The caller is responsible for all management of the connection life-cycle including closing the connection when no longer
        needed.
        """
        return await aioredis.Redis(pool_or_conn=self.redis_pool)

    def get_connection_sync(self, strict_client: bool = False) -> Union[redis.StrictRedis, redis.Redis]:
        """
        Synchronous method for creating a new Redis connection which will return a connected :py:class:`redis.Redis` or
        :py:class:`redis.StrictRedis` instance, based on the provided ``strict_client`` value.

        The caller is responsible for all management of the connection life-cycle including closing the connection when no longer
        needed.

        :param strict_client: if specified return an instance of :py:class:`redis.StrictRedis`. By default an configured instance
                              of :py:class:`redis.Redis` will be returned
        """

        conn_cls = redis.StrictRedis if strict_client else redis.Redis
        return conn_cls(connection_pool=self.redis_pool)

    get_connection = property(fget=lambda self: self.get_connection_async if self.is_async else self.get_connection_sync)

    def __init__(self, redis_pool: RedisPoolType, is_async: bool = None) -> None:
        """
        Simple ``ctor`` method for building ``RedisentHelper`` instance from a given ``RedisPoolType``

        If the pool is asynchronous, the ``is_async`` attribute must be ``True``. The inverse is true for non-asynchronous
        pool instances.

        If ``is_async`` is not provided (which defaults to ``None``) the default behavior is to determine if the helper should act
        asynchronously based on the class type of ``redis_pool``.

        This is important to keep in mind during testing since mocking the respective :py:mod:`redis` or :py:mod:`aioredis` internals
        might cause the default check here to incorrectly presume async or sync. The default check uses this to cover support
        for `fakeredis <https://pypi.org/project/fakeredis/>`_:

        .. code-block:: python

           if 'pytest' in sys.modules:
               import fakeredis

               logger.info('Running under pytest, checking for mocked redis/aioredis classes')
               is_async = not isinstance(redis_pool, (fakeredis.FakeConnection, redis.ConnectionPool,))
           else:
               is_async = isinstance(redis_pool, aioredis.ConnectionsPool)

        .. seealso::

           This method is called by :py:func:`RedisentHelper.build_pool_async` and:py:func:`RedisentHelper.build_pool_sync` also

        :param redis_pool: Redis connection pool helper should use
        :param is_async: if provided, indicates explicitly if the pool is asynchronous or not.
                         otherwise, the type of ``redis_pool`` will be used to determine this.
        """

        if is_async is None:
            if 'pytest' in sys.modules:
                import fakeredis

                logger.info('Running under pytest, checking for mocked redis/aioredis classes')
                is_async = not isinstance(redis_pool, (fakeredis.FakeConnection, redis.ConnectionPool,))
            else:
                is_async = isinstance(redis_pool, aioredis.ConnectionsPool)

        self.redis_pool = redis_pool
        self.is_async = is_async

    @classmethod
    def build_pool_sync(cls, redis_uri: str) -> redis.ConnectionPool:
        """
        Build a :py:class:`redis.ConnectionPool` instance from the given Redis URI

        This method uses ``redis.connection.ConnectionPool.from_url`` under the hood to build the connection pool

        :param redis_uri: URI of Redis server (will be prefixed with ``redis://`` if not present)
        """

        if not redis_uri.startswith('redis://'):
            redis_uri = f'redis://{redis_uri}'

        return redis.ConnectionPool.from_url(redis_uri)

    @classmethod
    async def build_pool_async(cls, redis_uri: str) -> aioredis.ConnectionsPool:
        """
        Build a :py:class:`aioredis.ConnectionsPool` instance from the given Redis URI

        This method uses :py:func:`aioredis.create_redis_pool`  under the hood to build the connection pool

        :param redis_uri: URI of Redis server (will be prefixed with ``redis://`` if not present)
        """

        if not redis_uri.startswith('redis://'):
            redis_uri = f'redis://{redis_uri}'

        return await aioredis.create_redis_pool(redis_uri)

    # Context managers for wrapped_redis helper
    @asynccontextmanager
    async def wrapped_redis_async(self, op_name: str = None):
        op_name = op_name or 'N/A'

        try:
            logger.debug(f'Executing Async Redis command for "{op_name}"...')
            yield self.redis_pool
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{op_name}": {ex}')
            raise RedisError(f'Redis Error executing "{op_name}": {ex}', base_exception=ex, related_command=op_name)

    @contextmanager
    def wrapped_redis_sync(self, op_name: str = None):
        op_name = op_name or 'N/A'

        try:
            r_conn = redis.Redis(connection_pool=self.redis_pool)
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

    wrapped_redis = property(fget=lambda self: self.wrapped_redis_async if self.is_async else self.wrapped_redis_sync)
