import logging
import pickle

from contextlib import contextmanager
from typing import List, Optional, Union, Mapping, Any

import redis

from redisent.errors import RedisError
from redisent.utils import REDIS_URL

RedisNativeValue = Union[bytes, float, int, str]

logger = logging.getLogger(__name__)


class RedisHelper:
    pool: redis.ConnectionPool

    def __init__(self, pool: redis.ConnectionPool = None, redis_url: str = REDIS_URL) -> None:
        if '://' in redis_url:
            redis_url = redis_url.split('://')[-1]

        self.pool = pool or redis.ConnectionPool(host=redis_url)

    @contextmanager
    def wrapper_redis(self, operation_name: str, use_pool: redis.ConnectionPool = None):
        pool = use_pool or self.pool

        try:
            conn = redis.Redis(connection_pool=pool, decode_responses=False)
        except Exception as ex:
            err_message = f'Redis Error building connection for "{operation_name}": {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex, extra_attrs={'op_name': operation_name})

        try:
            logger.debug(f'Running Redis operation "{operation_name}"')
            yield conn
        except Exception as ex:
            err_message = f'Redis Error running "{operation_name}": {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex, extra_attrs={'op_name': operation_name})

    @classmethod
    def parse_reponse(cls, value: bytes, ignore_failure: bool = False) -> Optional[Mapping[str, Any]]:
        try:
            return pickle.loads(value)
        except Exception as ex:
            err_message = f'Unable to decode Redis value as dictionary using pickle: {ex}'

            if not ignore_failure:
                logger.exception(err_message)
                raise RedisError(err_message, base_exception=ex)

            logger.warning(f'{err_message}. Ignoring.')
            return None

    def keys(self, pattern: str = None, decode_keys: bool = True, encoding: str = 'utf-8', use_pool: redis.ConnectionPool = None) -> List[str]:
        pattern = pattern or '*'
        op_name = f'keys(pattern="{pattern}")'

        with self.wrapper_redis(op_name, use_pool=use_pool) as redis_conn:
            redis_keys = redis_conn.keys(pattern)

        if not decode_keys:
            return redis_keys

        return [r_key.decode(encoding) for r_key in redis_keys]

    def exists(self, key: str, use_pool: redis.ConnectionPool = None):
        op_name = f'exists(key="{key})'

        with self.wrapper_redis(operation_name=op_name, use_pool=use_pool) as redis_conn:
            k_exists = redis_conn.exists(key)

        return True if k_exists and k_exists > 0 else False

    def get(self, key: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None) -> Optional[bytes]:
        op_name = f'exists(key="{key}") + get(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            err_message = 'Attempted GET on "{key}" which does not exist (missing_okay: {missing_okay})'
            logger.info(err_message)

            if missing_okay:
                return None

            raise RedisError(err_message, extra_attrs={'op_name': op_name})

        with self.wrapper_redis(operation_name=op_name, use_pool=use_pool) as redis_conn:
            return redis_conn.get(key)

    def set(self, key: str, value: RedisNativeValue, use_pool: redis.ConnectionPool = None) -> bool:
        op_name = f'set(key="{key}", value="...")'

        with self.wrapper_redis(operation_name=op_name, use_pool=use_pool) as redis_conn:
            res = redis_conn.set(key, value)

            return res and res > 0

    def delete(self, key: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None) -> Optional[bool]:
        op_name = f'delete(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            err_message = f'Attempted to DELETE key "{key}" which does not exist (missing_okay: {missing_okay})'
            logger.info(err_message)

            if missing_okay:
                return None

            raise RedisError(err_message, extra_attrs={'op_name': op_name})

        with self.wrapper_redis(operation_name=op_name, use_pool=use_pool) as redis_conn:
            res = redis_conn.delete(key)

            return res and res > 0
