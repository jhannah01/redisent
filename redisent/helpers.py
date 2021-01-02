from __future__ import annotations

import asyncio
import logging

import aioredis
import redis

from contextlib import contextmanager, asynccontextmanager
from decorator import decorator
from typing import List, Optional, Union, Mapping, Any

from redisent.errors import RedisError
from redisent.utils import RedisPoolType, REDIS_URL, RedisPrimitiveType

logger = logging.getLogger(__name__)


class RedisentHelper:
    redis_pool: RedisPoolType

    def __init__(self, redis_pool: RedisPoolType) -> None:
        self.redis_pool = redis_pool

    @classmethod
    def build_pool(cls, redis_url: str = REDIS_URL):
        raise NotImplementedError('Subclasses of RedisentHelper must implement the class build method')

    @classmethod
    def build(cls, redis_pool: RedisPoolType, use_async: bool = False) -> Union[BlockingRedisentHelper, AsyncRedisentHelper]:
        return BlockingRedisentHelper(redis_pool) if not use_async else AsyncRedisentHelper(redis_pool)


class BlockingRedisentHelper(RedisentHelper):
    @classmethod
    def build_pool(cls, redis_url: str = REDIS_URL) -> BlockingRedisentHelper:
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
            raise RedisError(err_message, base_exception=ex, related_command=op_name)

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
                raise RedisError(f'Error setting key "{key}" in Redis: No response', related_command=op_name)

            return True

    def get(self, key: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None, return_type: RedisPrimitiveType = None) -> Optional[RedisPrimitiveType]:
        op_name = f'get(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist', related_command=op_name)

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            if return_type:
                r_conn.set_response_callback('HGET', return_type)
                encoding = None
            return r_conn.get(key)

    def delete(self, key: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None) -> Optional[bool]:
        op_name = f'delete(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist', related_command=op_name)

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
            r_conn.hset(key, key=name, value=value)
            return True

    def hget(self, key: str, name: str, missing_okay: bool = False, use_pool: redis.ConnectionPool = None, return_bytes: bool = False, use_return_type: RedisPrimitiveType = None) -> Optional[Union[bytes, RedisPrimitiveType]]:
        op_name = f'hget(key="{key}", name="{name}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist', related_command=op_name)

        if not self.hexists(key, name, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry', related_command=op_name)

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            if use_return_type:
                r_conn.set_response_callback('HGET', use_return_type)
                return_bytes = False

            value = r_conn.hget(key, name)

        return bytes(value) if return_bytes else value

    def hgetall(self, key: str, encoding: str = 'utf-8', missing_okay: bool = False, use_pool: redis.ConnectionPool = None) -> Optional[Mapping[str, Any]]:
        op_name = f'hgetall(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to find hash key "{key}" in Redis: No such key', related_command=op_name)

        with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            entries = r_conn.hgetall(key)

        if not entries:
            return {}

        if not encoding:
            return entries

        return {ent_name.decode(encoding): ent_value for ent_name, ent_value in entries.items()}


class AsyncRedisentHelper(RedisentHelper):
    redis_pool: aioredis.ConnectionsPool

    @classmethod
    async def build_pool(cls, redis_url: str = REDIS_URL) -> AsyncRedisentHelper:
        pool = await aioredis.create_redis_pool(redis_url)
        return AsyncRedisentHelper(pool)

    @asynccontextmanager
    async def wrapped_redis(self, op_name: str, use_pool: aioredis.ConnectionsPool = None, close_after: bool = None):
        pool = use_pool or self.redis_pool

        try:
            logger.debug(f'Executing Async Redis command for "{op_name}"...')
            yield pool
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{op_name}": {ex}')
            raise RedisError(f'Redis Error executing "{op_name}": {ex}', base_exception=ex, related_command=op_name)
        finally:
            if close_after:
                if pool is None:
                    logger.warning(
                        f'Received request to close after executing "{op_name}" but pool provided is helper-wide pool.')
                else:
                    pool.close()
                    await pool.wait_closed()

    async def keys(self, pattern: str = None, decode_keys: str = 'utf-8', use_pool: aioredis.ConnectionsPool = None) -> List[str]:
        pattern = pattern or '*'
        op_name = f'keys(pattern="{pattern}")'

        async with self.wrapped_redis(operation_name=op_name, use_pool=use_pool) as r_conn:
            return await r_conn.keys(pattern, encoding=decode_keys)

    async def exists(self, key: str, use_pool: aioredis.ConnectionsPool = None) -> bool:
        op_name = f'exists(key="{key}")'
        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return (await r_conn.exists(key)) == 1

    async def set(self, key: str, value: RedisPrimitiveType, use_pool: aioredis.ConnectionsPool = None) -> bool:
        op_name = f'set(key="{key}", value="...")'

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = await r_conn.set(key, value)

            if not res:
                raise RedisError(f'Error setting key "{key}" in Redis: No response', related_command=op_name)

            return True

    async def get(self, key: str, missing_okay: bool = False, encoding: str = None, use_pool: aioredis.ConnectionsPool = None, use_return_type: RedisPrimitiveType = None) -> Optional[RedisPrimitiveType]:
        op_name = f'get(key="{key}")'

        if not await self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')


        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            if use_return_type:
                # r_conn.RESPONSE_CALLBACKS['GET'] = use_return_type
                encoding = None
            return await r_conn.get(key, encoding=encoding)

    async def delete(self, key: str, missing_okay: bool = False, use_pool: aioredis.ConnectionsPool = None) -> Optional[bool]:
        op_name = f'delete(key="{key}")'

        if not await self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = await r_conn.delete(key)

        return res and res > 0

    # Hash-related functions
    async def hkeys(self, key: str, decode_keys: str = 'utf-8', use_pool: aioredis.ConnectionsPool = None) -> List[str]:
        op_name = f'hkeys(key="{key}")'

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return await r_conn.hkeys(key, encoding=decode_keys)

    async def hexists(self, key: str, name: str, use_pool: aioredis.ConnectionsPool = None) -> bool:
        op_name = f'hexists("{key}", "{name}")'

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return await r_conn.hexists(key, name)

    async def hset(self, key: str, name: str, value: RedisPrimitiveType, use_pool: aioredis.ConnectionsPool = None) -> bool:
        op_name = f'hset(key="{key}", name="{name}", value="...")'

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            res = await r_conn.hset(key, name, value)

            if not res:
                raise RedisError(f'Error setting hash key "{key}" in Redis: No response', related_command=op_name)

            return True

    async def hget(self, key: str, name: str, missing_okay: bool = False, encoding: str = None, use_pool: aioredis.ConnectionsPool = None, use_return_type: RedisPrimitiveType = None) -> Optional[RedisPrimitiveType]:
        op_name = f'hget(key="{key}", name="{name}")'

        if not await self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        if not await self.hexists(key, name, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry', related_command=op_name)

        if use_return_type:
            encoding = None

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            value = await r_conn.hget(key, name, encoding=encoding)

        if not value:
            return None

        return use_return_type(value) if use_return_type else value


RedisHelperTypes = Union[BlockingRedisentHelper, AsyncRedisentHelper]
