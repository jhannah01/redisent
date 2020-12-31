from __future__ import annotations

import aioredis
import logging
import pickle

from redis import ConnectionPool, Redis, StrictRedis

from contextlib import asynccontextmanager, contextmanager
from typing import Any, List, Mapping, Optional, Union, Type

from redisent import RedisEntry
from redisent.errors import RedisError
from redisent.utils import REDIS_URL

logger = logging.getLogger(__name__)

AsyncRedisPoolConnType = Union[aioredis.Redis, aioredis.ConnectionsPool, aioredis.RedisConnection]
RedisPoolConnType = Union[Redis, StrictRedis, ConnectionPool]
AnyPoolConnType = Union[AsyncRedisPoolConnType, RedisPoolConnType]

RedisNativeValue = Union[bytes, float, int, str]
RedisValueType = Union[RedisNativeValue, Mapping[str, Any], RedisEntry]


class BaseRedisHelper:
    redis_pool: AnyPoolConnType

    def __init__(self, redis_pool: AnyPoolConnType) -> None:
        self.redis_pool = redis_pool

    @classmethod
    def build(cls, redis_pool: AnyPoolConnType = None, redis_url: str = REDIS_URL):
        redis_pool = redis_pool or cls.build_pool(redis_url)
        return cls(redis_pool)

    @classmethod
    def build_pool(cls, redis_url: str = REDIS_URL):
        raise NotImplementedError('Subclasses of BaseRedisHelper must implement the class method "build_pool"')

    @classmethod
    def convert_keys_to_unicode(cls, entry: Mapping[Union[str, bytes], Any]):
        return {ent_name.decode('utf-8') if isinstance(ent_name, bytes) else ent_name: ent_value for
                ent_name, ent_value in entry.items()}

    @classmethod
    def encode_entry(cls, entry: Union[RedisEntry, Mapping[str, Any]]) -> bytes:
        if isinstance(entry, RedisEntry):
            entry = entry.as_dict(include_redis_attrs=True, include_internal_attrs=False)

        redis_id, redis_name = entry.get('redis_id', None), entry.get('redis_name', None)

        try:
            return pickle.dumps(entry)
        except Exception as ex:
            err_details = f'Error while encoding entry for key "{redis_id or "Unknown"}'
            if redis_name:
                err_details = f'{err_details} (entry: {redis_name})'

            err_details = f'{err_details} using Pickle: {ex}'

            raise RedisError(err_details, base_exception=ex, extra_attrs={'entry': entry})

    @classmethod
    def decode_entry(cls, entry_bytes: bytes, key: str, name: str = None) -> Mapping[str, Any]:
        try:
            return pickle.loads(entry_bytes)
        except Exception as ex:
            name_str = f' (entry: {name})' if name else ''
            err_message = f'Error while decoding entry for key "{key}"{name_str} using Pickle: {ex}'
            raise RedisError(err_message, base_exception=ex)

    @classmethod
    def encode_redis_value(cls, value: RedisValueType) -> RedisNativeValue:
        if isinstance(value, RedisEntry):
            value = value.as_dict(include_redis_attrs=True)

        if isinstance(value, Mapping):
            value = cls.encode_entry(value)

        return value

    @classmethod
    def decode_redis_value(cls, value: RedisNativeValue, key: str, name: str = None, encoding: str = 'utf-8') -> RedisValueType:
        if isinstance(value, bytes):
            return cls.decode_entry(value, key, name=name)

        if isinstance(value, bytes) and encoding:
            value = value.decode(encoding)

        return value

    def parse_response(self, value: bytes, key: str, name: str = None, entry_cls: RedisEntry = None, decode_value: bool = True, encoding: str = 'utf-8'):
        try:
            val_mapping = pickle.loads(value)
        except pickle.UnpicklingError:
            val_mapping = None

        if val_mapping:
            return val_mapping if not entry_cls else entry_cls.from_dict(self, **val_mapping)

        return value.decode(encoding) if decode_value else value


class RedisHelper(BaseRedisHelper):
    redis_pool: ConnectionPool

    @classmethod
    def build_pool(cls, redis_url: str = REDIS_URL) -> ConnectionPool:
        if redis_url.startswith('redis://'):
            redis_url = redis_url.split('://', 1)[-1]

        return ConnectionPool(host=redis_url)

    @contextmanager
    def wrapped_redis(self, operation_name: str = None, conn: RedisPoolConnType = None, close_after: bool = None):
        operation_name = operation_name or 'Unknown'

        try:
            if conn:
                if isinstance(conn, ConnectionPool):
                    redis_conn = Redis(connection_pool=conn)
                elif isinstance(conn, Redis):
                    redis_conn = conn
                else:
                    raise ValueError(f'Provided value for "conn" is not a valid Redis connection or pool: "{conn}"')
            else:
                redis_conn = Redis(connection_pool=self.redis_pool)
                close_after = True
        except Exception as ex:
            err_message = f'Failed to create new Redis connection: {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex)

        try:
            logger.debug(f'Executing Redis command for "{operation_name}"...')
            yield redis_conn
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{operation_name}": {ex}')
            raise RedisError(f'Redis Error executing "{operation_name}": {ex}', base_exception=ex,
                             related_command=operation_name)
        finally:
            logger.debug(f'Finished execution of wrapped_redis command for {operation_name}')
            if conn and close_after is not False and isinstance(conn, ConnectionPool):
                logger.debug(f'Based on "close_after" value, closing Redis connection')
                redis_conn.connection_pool.release(conn)

    def keys(self, pattern: str = None, encoding: str = 'utf-8', conn: RedisPoolConnType = None) -> List[str]:
        pattern = pattern or '*'

        with self.wrapped_redis(operation_name=f'keys("{pattern}")', conn=conn) as redis_conn:
            key_values = redis_conn.keys(pattern)

            if key_values and encoding:
                return [k_name.decode(encoding) for k_name in key_values]

            return key_values

    def hkeys(self, key: str, encoding: str = 'utf-8', conn: RedisPoolConnType = None) -> List[str]:
        with self.wrapped_redis(operation_name=f'hkeys("{key}")', conn=conn) as redis_conn:
            hkey_values = redis_conn.hkeys(key)

            if hkey_values and encoding:
                return [hk_name.decode(encoding) for hk_name in hkey_values]

            return hkey_values

    def exists(self, key: str, conn: RedisPoolConnType = None) -> bool:
        with self.wrapped_redis(operation_name=f'exists("{key}")', conn=conn) as redis_conn:
            return redis_conn.exists(key)

    def hexists(self, key: str, name: str, conn: RedisPoolConnType = None) -> bool:
        with self.wrapped_redis(operation_name=f'hexists("{key}", "{name}")', conn=conn) as redis_conn:
            return redis_conn.hexists(key, name)

    def hlen(self, key: str, conn: RedisPoolConnType = None) -> Optional[int]:
        if not self.exists(key, conn=conn):
            logger.warning(f'Requested hlen("{key}") for non-existent key. Returning None')
            return None

        with self.wrapped_redis(operation_name=f'hlen("{key}")', conn=conn) as redis_conn:
            return redis_conn.hlen(key)

    def llen(self, key: str, conn: RedisPoolConnType = None) -> Optional[int]:
        op_name = f'llen(key="{key}")'

        if not self.exists(key, conn=conn):
            logger.warning(f'Request llen for non-existent key. Returning None')
            return None

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            return redis_conn.llen(key)

    def lindex(self, key: str, index: int, decode_value: bool = False, decode_response: bool = True, encoding: str = 'utf-8',
               conn: RedisPoolConnType = None) -> Optional[Union[bytes, Mapping[str, Any]]]:
        op_name = f'lindex(key="{key}", index="{index}")'

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            if not redis_conn.exists(key):
                logger.warning(f'Requested {op_name} on non-existent key. Returning None')
                return None

            ent_bytes = redis_conn.lindex(key, index)

        if not ent_bytes:
            logger.warning(f'Received no data for lindex of "{key}" at "{index}"')
            return None

        if decode_value:
            return self.decode_entry(ent_bytes, key=key)
        elif decode_response:
            return ent_bytes.decode(encoding)

        return ent_bytes

    def lrange(self, key: str, start: int, end: int, decode_response: bool = True, encoding: str = 'utf-8', conn: RedisPoolConnType = None) -> Optional[List[str]]:
        op_name = f'lrange(key="{key}", start={start}, end={end})'

        if not self.exists(key, conn=conn):
            logger.warning(f'Requested {op_name} on non-existent key. Returning None')
            return None

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            if not redis_conn.exists(key):
                logger.warning(f'Requested {op_name} on non-existent key. Returning None')
                return None

            ent_bytes = redis_conn.lindex(key, start, end)

        if not ent_bytes:
            logger.warning(f'Received no data for lrange of "{key}" (start: {start}, end: {end})')
            return None

        if decode_response:
            return ent_bytes.decode(encoding)

        return ent_bytes

    '''
    def linsert
    def lpop
    def lpush
    '''

    '''
    def lrem
    def lset
    def ltrim
    '''

    def set(self, key: str, value: RedisValueType, conn: RedisPoolConnType = None) -> bool:
        op_name = f'set(key="{key}", value="...")'

        if isinstance(value, (RedisEntry, Mapping)):
            value = self.encode_entry(value)

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            res = redis_conn.set(key, value)

            if not res:
                raise RedisError(f'Error setting key "{key}" in Redis: No response')

            return True

    def hset(self, key: str, name: str, value: RedisValueType, conn: RedisPoolConnType = None) -> bool:
        op_name = f'hset(key="{key}", name="{name}", value="...")'

        value = self.encode_entry(value) if isinstance(value, (RedisEntry, Mapping)) else value

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            res = redis_conn.hset(key, name, value)

            if not res:
                raise RedisError(f'Error setting hash key "{key}" in Redis: No response')

            return True

    async def get(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, encoding: str = 'utf-8', conn: RedisPoolConnType = None):
        op_name = f'get(key="{key}")'

        if not self.exists(key, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            ent_value = redis_conn.get(key)

        if decode_value:
            return self.decode_entry(ent_value, key)

        return ent_value.decode(encoding) if decode_response else ent_value

    async def hget(self, key: str, name: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True,
                   encoding: str = 'utf-8', conn: RedisPoolConnType = None):
        op_name = f'hset(key="{key}", name="{name}")'

        if not self.exists(key, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        if not self.hexists(key, name, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry')

        with self.wrapped_redis(operation_name=op_name, conn=conn) as redis_conn:
            ent_bytes = redis_conn.hget(key, name)

        if decode_value:
            return self.decode_entry(ent_bytes, key, name=name)

        return ent_bytes.decode(encoding) if decode_response else ent_bytes

    def delete(self, key: str, missing_okay: bool = False, conn: RedisPoolConnType = None) -> Optional[bool]:
        if not self.exists(key, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        with self.wrapped_redis(operation_name='delete("{key}")', conn=conn) as redis_conn:
            return redis_conn.delete(key) > 0

    def hdelete(self, key: str, name: str, missing_okay: bool = False, conn: RedisPoolConnType = None) -> Optional[bool]:
        if not self.hexists(key, name, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry for "{name}" in key "{key}": No such entry in "{key}"')

        with self.wrapped_redis(operation_name='hdel("{key}", "{name}")', conn=conn) as redis_conn:
            return redis_conn.hdel(key, name) > 0

    def hgetall(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, conn: RedisPoolConnType = None) -> Optional[Mapping[str, Any]]:
        if not self.exists(key, conn=conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entries from non-existent key "{key}"')

        with self.wrapped_redis(operation_name='hgetall("{key}")', conn=conn) as redis_conn:
            entries = redis_conn.hgetall(key)

            if not decode_value:
                return self.convert_keys_to_unicode(entries) if decode_response else entries

            try:
                ent_objs = {ent_name: pickle.loads(ent_value) for ent_name, ent_value in entries.items()}
                return self.convert_keys_to_unicode(ent_objs)
            except Exception as ex:
                raise RedisError(f'Error while decoding Redis hash entry "{key}" using Pickle: {ex}', base_exception=ex, extra_attrs={'entries': entries})


class AsyncRedisHelper(BaseRedisHelper):
    redis_pool: AsyncRedisPoolConnType
    redis_url: str

    def __del__(self):
        if self.redis_pool:
            logger.info('Cleaning up Redis pool on deletion')

            self.redis_pool.close()

    @classmethod
    async def build(cls, redis_pool: AsyncRedisPoolConnType = None, redis_url: str = REDIS_URL):
        redis_pool = redis_pool or await cls.build_pool(redis_url)
        return AsyncRedisHelper(redis_pool)

    @classmethod
    async def build_pool(cls, redis_url: str = REDIS_URL):
        try:
            return await aioredis.create_redis_pool(redis_url)
        except Exception as ex:
            raise RedisError(f'Error attempting to connect to "{redis_url}": {ex}', base_exception=ex)

    @asynccontextmanager
    async def wrapped_redis(self, operation_name: str = None, pool: AsyncRedisPoolConnType = None, close_after: bool = False):
        operation_name = operation_name or "Unknown"
        pool = pool or self.redis_pool

        try:
            logger.debug(f'Executing Async Redis command for "{operation_name}"...')
            yield pool
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{operation_name}": {ex}')
            raise RedisError(f'Redis Error executing "{operation_name}": {ex}', base_exception=ex,
                             related_command=operation_name)
        finally:
            if close_after:
                if pool == self.redis_pool:
                    logger.warning(f'Received request to close after executing "{operation_name}" but pool provided is helper-wide pool.')
                else:
                    pool.close()
                    await pool.wait_closed()

    async def keys(self, pattern: str = None, encoding: str = 'utf-8', pool: AsyncRedisPoolConnType = None) -> List[str]:
        pattern = pattern or '*'

        async with self.wrapped_redis(operation_name=f'keys("{pattern}")', pool=pool) as redis_conn:
            return await redis_conn.keys(pattern, encoding=encoding)

    async def hkeys(self, key: str, encoding: str = 'utf-8', pool: AsyncRedisPoolConnType = None) -> List[str]:
        async with self.wrapped_redis(operation_name=f'hkeys("{key}")', pool=pool) as redis_conn:
            return await redis_conn.hkeys(key, encoding=encoding)

    async def exists(self, key: str, pool: AsyncRedisPoolConnType = None) -> bool:
        async with self.wrapped_redis(operation_name=f'exists("{key}")', pool=pool) as redis_conn:
            return (await redis_conn.exists(key)) == 1

    async def hexists(self, key: str, name: str, pool: AsyncRedisPoolConnType = None) -> bool:
        async with self.wrapped_redis(operation_name=f'hexists("{key}", "{name}")', pool=pool) as redis_conn:
            return await redis_conn.hexists(key, name)

    async def set(self, key: str, entry: Any, encode_value: bool = True, pool: AsyncRedisPoolConnType = None) -> bool:
        if encode_value:
            try:
                if not isinstance(entry, bytes):
                    entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error while encoding entry for "{key}" using Pickle: {ex}')

        async with self.wrapped_redis(operation_name=f'set("{key}", ...)', pool=pool) as redis_conn:
            res = await redis_conn.set(key, entry)

            if not res:
                raise RedisError(f'Error setting key "{key}" in Redis: No response')

            return True

    async def hset(self, key: str, name: str, entry: Any, encode_value: bool = True, pool: AsyncRedisPoolConnType = None) -> bool:
        if encode_value:
            try:
                entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error encoding entry for "{key}" -> "{name}" from Pickle: {ex}')

        async with self.wrapped_redis(pool=pool, operation_name=f'hset("{key}", "{name}", ...)') as redis_conn:
            res = await redis_conn.hset(key, name, entry)

            if not res:
                raise RedisError(f'Error setting hash key "{key}" in Redis: No response')

            return True

    async def get(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, pool: AsyncRedisPoolConnType = None):
        if not await self.exists(key, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        async with self.wrapped_redis(pool=pool, operation_name=f'get("{key}")') as redis_conn:
            ent_bytes = await redis_conn.get(key, encoding=None)

        if not decode_value:
            return ent_bytes if not decode_response or not isinstance(ent_bytes, bytes) else ent_bytes.decode('utf-8')

        try:
            return pickle.loads(ent_bytes)
        except Exception as ex:
            raise RedisError(f'Error while decoding Redis entry "{key}" using Pickle: {ex}')

    async def hget(self, key: str, name: str, missing_okay: bool = False, decode_value: bool = True, pool: AsyncRedisPoolConnType = None):
        if not await self.exists(key, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        if not await self.hexists(key, name, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry')

        async with self.wrapped_redis(pool=pool, operation_name=f'hget("{key}", "{name}")') as redis_conn:
            ent_bytes = await redis_conn.hget(key, name)

        if not decode_value:
            return ent_bytes

        try:
            return pickle.loads(ent_bytes)
        except Exception as ex:
            raise RedisError(f'Error while decoding Redis entry "{name}" in hash key "{key}" using Pickle: {ex}')

    async def delete(self, key: str, missing_okay: bool = False, pool: AsyncRedisPoolConnType = None) -> Optional[bool]:
        if not await self.exists(key, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        async with self.wrapped_redis(pool=pool, operation_name='delete("{key}")') as redis:
            return await redis.delete(key) > 0

    async def hdelete(self, key: str, name: str, missing_okay: bool = False, pool: AsyncRedisPoolConnType = None) -> Optional[bool]:
        if not await self.hexists(key, name, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry for "{name}" in key "{key}": No such entry in "{key}"')

        async with self.wrapped_redis(pool=pool, operation_name='hdel("{key}", "{name}")') as redis:
            return await redis.hdel(key, name) > 0

    async def hgetall(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, pool: AsyncRedisPoolConnType = None) -> Optional[Mapping[str, Any]]:
        if not await self.exists(key, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entries from non-existent key "{key}"')

        async with self.wrapped_redis(pool=pool, operation_name='hgetall("{key}")') as redis_conn:
            entries = await redis_conn.hgetall(key)

            if not decode_value:
                return self.convert_keys_to_unicode(entries) if decode_response else entries

            try:
                ent_objs = {ent_name: pickle.loads(ent_value) for ent_name, ent_value in entries.items()}
                return self.convert_keys_to_unicode(ent_objs)
            except Exception as ex:
                raise RedisError(f'Error while decoding Redis hash entry "{key}" using Pickle: {ex}', base_exception=ex, extra_attrs={'entries': entries})
