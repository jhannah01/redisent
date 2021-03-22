from __future__ import annotations

import logging
import pickle
import redis
import functools

from contextlib import contextmanager
from typing import Callable, Union, List, Any, Optional

from redisent.errors import RedisError
from redisent.types import RedisType, is_redislite_instance

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: redis.ConnectionPool
    use_redis: RedisType = None

    @classmethod
    def handle_decode_attempt(cls, result_value, use_encoding: str = None, decode_handler: Callable[[Any], Optional[Any]] = None):
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
            except (pickle.PickleError, TypeError,):
                if decode_handler:
                    return decode_handler(value)
                elif use_encoding and hasattr(value, 'decode'):
                    return value.decode(use_encoding)

                return value

        if isinstance(result_value, list):
            result_value = [decode_value(ent) for ent in result_value]
        elif isinstance(result_value, dict):
            results = {}
            for ent_name, ent_value in result_value.items():
                ent_name = ent_name.decode(use_encoding) if use_encoding and hasattr(ent_name, 'decode') else ent_name
                results[ent_name] = decode_value(ent_value)

            return results
        elif use_encoding and hasattr(result_value, 'decode'):
            result_value = result_value.decode(use_encoding)

        return result_value

    @classmethod
    def decode_entries(cls, use_encoding: str = None, first_handler: Callable = None, final_handler: Callable = None):
        """
        Decorator used for automatically attempting to decode the returned value of a method using the static method ``handle_decode_attempt``

        This is helpful for automatically returning :py:class:`redisent.models.RedisEntry` instances and / or the opportunity to interact with
        the results via the two ``Callable`` arguments ``first_handler`` and ``final_handler``.

        If provided, the ``first_handler`` is used first, prior to attempting to using the ``handle_decode_attempt`` static method. Finally,
        if provided, the ``final_handler`` will be called prior to passing the possibly decoded response back to the caller.

        :param use_encoding: if provided, indicates the results should be decoded using the provided encoding (generally ``utf-8``)
        :param first_handler: first callback handler to invoke __prior__ to attempting to decode the result
        :param final_handler: final callback handler to invoke __after__ attempting to decode the result
        """

        def _wrapper(func):
            @functools.wraps(func)
            def _decode_wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                return first_handler(res) if first_handler else cls.handle_decode_attempt(res, use_encoding, decode_handler=final_handler)

            return _decode_wrapper

        return _wrapper

    def get_connection(self, strict_client: bool = False) -> RedisType:
        """
        Synchronous method for creating a new Redis connection which will return a connected ``redis.Redis`` or ``redis.StrictRedis`` instance,
        based on the provided ``strict_client`` value.

        The caller is responsible for all management of the connection life-cycle including closing the connection when no longer needed.

        :param strict_client: if specified return an instance of ``redis.StrictRedis``. By default an configured instance of  ``redis.Redis``
                              will be returned
        """

        if self.use_redis:
            return self.use_redis

        conn_cls = redis.StrictRedis if strict_client else redis.Redis
        return conn_cls(connection_pool=self.redis_pool)

    def __init__(self, redis_pool: redis.ConnectionPool, use_redis: RedisType = None) -> None:
        """
        Simple ``ctor`` method for building ``RedisentHelper`` instance from a given ``RedisPoolType``

        :param redis_pool: Redis connection pool helper should use
        :param use_redis: primarily for testing, this instance of one of the ``redis`` or ``redislite`` classes in the type ``redisent.types.RedisType``.
                          if provided, the :py:func:`RedisentHelper.get_connection` method will return it instead of building a new one with the
                          provided ``redis_pool`` (which is ignored)
        """

        if is_redislite_instance(redis_pool):
            use_redis = redis_pool
            redis_pool = None

        self.redis_pool = redis_pool
        self.use_redis = use_redis

    @classmethod
    def build_pool(cls, redis_uri: str) -> redis.ConnectionPool:
        """
        Build a ``redis.ConnectionPool`` instance from the given Redis URI

        This method uses ``redis.connection.ConnectionPool.from_url`` under the hood to build the connection pool

        :param redis_uri: URI of Redis server (will be prefixed with ``redis://`` if not present)
        """

        if not redis_uri.startswith('/') and not redis_uri.startswith('redis://'):
            redis_uri = f'redis://{redis_uri}'

        return redis.ConnectionPool.from_url(redis_uri)

    @contextmanager
    def wrapped_redis(self, op_name: str = None):
        op_name = op_name or 'N/A'

        try:
            r_conn = self.get_connection()
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

    def keys(self, use_pattern: str = None, redis_id: str = None, use_encoding: str = None) -> List[str]:
        """
        Synchronous method responsible for enumerating key values in Redis for hash and non-hash entries alike

        If ``use_pattern`` and ``redis_id`` are not provided, this method will use ``KEYS(*)`` to lookup all Redis keys.

        Otherwise, if ``redis_id`` is provided, a lookup of ``HKEYS(redis_id)`` will be done.

        Finally, if ``use_pattern`` is provided, ``KEYS(use_pattern)`` will be used

        :param use_pattern: if provided, use this value instead of ``*`` with ``KEYS``
        :param redis_id: if provided, use ``HKEYS(redis_id)`` to lookup keys in the hash entry
        :param use_encoding: if provided, key values will be decoded using the value (generally this would be ``utf-8``)
        """

        if redis_id:
            op_name = f'hkeys("{redis_id}")'
        else:
            use_pattern = use_pattern or '*'
            op_name = f'keys("{use_pattern}")'

        with self.wrapped_redis(op_name) as r_conn:
            found_keys = r_conn.hkeys(redis_id) if redis_id else r_conn.keys(use_pattern)

        return found_keys if not use_encoding else [k_val.decode(use_encoding) for k_val in found_keys]

    def exists(self, redis_id: str, redis_name: str = None) -> bool:
        """
        Synchronous method for checking if a given ``redis_id`` (and optional ``redis_name`` value, if provided) actually exists in Redis

        :param redis_id: the Redis ID for entry
        :param redis_name: if provided, attempt to lookup hashmap based on this value
        """

        op_name = f'hexists("{redis_id}", "{redis_name}")' if redis_name else f'exists("{redis_id}")'
        with self.wrapped_redis(op_name) as r_conn:
            res = r_conn.hexists(redis_id, redis_name) if redis_name else r_conn.exists(redis_id)
            return True if res else False
