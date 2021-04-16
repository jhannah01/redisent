from __future__ import annotations

import logging
import pickle
import redis
import functools

from contextlib import contextmanager
from typing import Callable, List, Any, Optional, cast

from redisent.errors import RedisError
from redisent.common import RedisType, RedisPoolType

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType
    use_redis: Optional[RedisType]

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

    def __init__(self, redis_pool: RedisPoolType, use_redis: RedisType = None) -> None:
        """
        Simple ``ctor`` method for building ``RedisentHelper`` instance from a given ``RedisPoolType``

        :param redis_pool: Redis connection pool helper should use
        :param use_redis: primarily for testing, this instance of one of the ``redis`` or ``redislite`` classes in the type ``redisent.common.RedisType``.
                          if provided, the :py:func:`RedisentHelper.get_connection` method will return it instead of building a new one with the
                          provided ``redis_pool`` (which is ignored)
        """

        self.redis_pool = redis_pool
        self.use_redis = use_redis

    @classmethod
    def build_pool(cls, redis_uri: str) -> RedisPoolType:
        """
        Build a ``RedisPoolType`` instance from the given Redis URI

        This method uses ``redis.connection.ConnectionPool.from_url`` under the hood to build the connection pool

        :param redis_uri: URI of Redis server (will be prefixed with ``redis://`` if not present)
        """

        if not redis_uri.startswith('/') and not redis_uri.startswith('redis://'):
            redis_uri = f'redis://{redis_uri}'

        return RedisPoolType.from_url(redis_uri)

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
        :param use_encoding: if provided, use this charset to decode bytes response (default is ``utf-8``)
        """

        use_encoding = use_encoding or 'utf-8'

        if redis_id:
            op_name = f'hkeys("{redis_id}")'
        else:
            use_pattern = use_pattern or '*'
            op_name = f'keys("{use_pattern}")'

        with self.wrapped_redis(op_name) as r_conn:
            found_keys = r_conn.hkeys(redis_id) if redis_id else r_conn.keys(use_pattern)

        return [k_val.decode(use_encoding) for k_val in found_keys]

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

    def entry_type(self, redis_id: str, check_exists: bool = True) -> Optional[str]:
        """
        Determine the Redis type of the provided ``redis_id`` entry

        :param redis_id: the Redis ID for entry
        :param check_exists: if set, use the :py:func:`RedisentHelper.exists` method to validate the entry exists or return ``None``
        """

        if check_exists and not self.exists(redis_id):
            logger.warning(f'Request for type of "{redis_id}" failed: No such entry')
            return None

        with self.wrapped_redis(f'type("{redis_id}")') as r_conn:
            ent_type = r_conn.type(redis_id)

        return cast(str, ent_type.decode('utf-8'))

    def get(self, redis_id: str, redis_name: str = None, throw_error: bool = True) -> Optional[Any]:
        """
        Method for fetching Redis entry based on ``redis_id`` and an optional ``redis_name`` value

        If ``redis_name`` is also provided, the hash get (``HGET``) method will be used to fetch the hashmap entry requested

        :param redis_id: the Redis ID for entry
        :param redis_name: if provided, attempt to lookup hashmap based on this value
        :param throw_error: if set, a :py:exc:`redisent.errors.RedisError` exception will be raised if the entry cannot be fetched,
                            otherwise ``None`` will be returned
        """

        if not self.exists(redis_id, redis_name=redis_name):
            red_ent = f'"{redis_id}"' if not redis_name else f'entry "{redis_name}" in hashmap "{redis_id}"'
            err_message = f'Fetch for {red_ent} failed: No such entry / key'

            if throw_error:
                extra_attrs = {'redis_id': redis_id, 'redis_name': redis_name}
                raise RedisError(err_message, extra_attrs=extra_attrs)

            logger.error(err_message)
            return None

        entry_type = self.entry_type(redis_id)
        use_getall = False

        if entry_type == 'hash':
            is_hash = True

            if not redis_name:
                use_getall = True
                op_name = f'hgetall("{redis_id}")'
            else:
                op_name = f'hget("{redis_id}", "{redis_name}")'
        else:
            is_hash = False
            op_name = f'get("{redis_id}")'

        with self.wrapped_redis(op_name) as r_conn:
            if is_hash:
                return r_conn.hget(redis_id, redis_name) if not use_getall else r_conn.hgetall(redis_id)

            return r_conn.get(redis_id)

    def set(self, redis_id: str, value: Any, redis_name: str = None, check_exists_type: bool = True) -> bool:
        """
        Method for storing a value in Redis as ``redis_id`` and optional ``redis_name`` for storing hashmap data

        If ``redis_name`` is alos provided, this operation will use ``HSET`` to create a hashmap. Otherwise ``SET`` is used
        and ``value`` must be an instance of one of the supported ``redisent.common.RedisType`` types.

        :param redis_id: the Redis ID for entry
        :param value: value to be stored in Redis. If this is not a hashmap, it must be a Redis primitive type of ``RedisType``
        :param redis_name: if provided, store this entry as a hashmap using both ``redis_id`` and ``redis_name``
        :returns: bool indicating if there the value was being set for the first time (i.e. ``True`` means it was not previously set)
        """

        if redis_name:
            is_hash = True
            op_name = f'hset("{redis_id}", "{redis_name}", "{value}")'
        else:
            is_hash = False
            op_name = f'set("{redis_id}", "{value}")'

        if check_exists_type and self.exists(redis_id, redis_name=redis_name):
            entry_type = self.entry_type(redis_id)
            if is_hash and entry_type != 'hash':
                raise RedisError(f'Type mismatch when attempting to overwrite Redis entry for "{redis_id}": Entry is a "{entry_type}", not hash map')

            if not is_hash and entry_type == 'hash':
                raise RedisError(f'Type mismatch when attempting to overwrite Redis entry for "{redis_id}": Entry is a hash map')

        with self.wrapped_redis(op_name) as r_conn:
            res = r_conn.hset(redis_id, redis_name, value) if is_hash else r_conn.set(redis_id, value)

        return True if res else False

    def delete(self, redis_id: str, redis_name: str = None, check_exists: bool = True) -> Optional[bool]:
        """
        Method for deleting stored Redis entries based on provided ``redis_id`` and optional hashmap name ``redis_name``

        :param
        :param redis_id: the Redis ID for entry to remove
        :param redis_name: if provided, attempt to delete the named entry in the hashmap based on this value
        :param check_exists: if set, use the :py:func:`RedisentHelper.exists` method to validate the entry exists
        :returns: bool indicating if the entry was deleted. if ``check_exists`` fails, ``None`` will be returned
        """

        if redis_name:
            is_hash = True
            op_name = f'hdel"{redis_id}", "{redis_name}")'
        else:
            is_hash = False
            op_name = f'set("{redis_id}")'

        if check_exists and not self.exists(redis_id, redis_name=redis_name):
            red_ent = '"{redis_id}"' if not is_hash else 'key "{redis_name}" of hashmap "{redis_id}"'
            logger.warning(f'Unable to delete Redis entry for {red_ent}: No such entry / key')
            return None

        with self.wrapped_redis(op_name) as r_conn:
            res = r_conn.hdel(redis_id, redis_name) if redis_name else r_conn.delete(redis_id)

        return True if res else False
