from __future__ import annotations

from typing import List, Optional

import aioredis
import logging

from contextlib import asynccontextmanager

from redisent.helpers import RedisentHelper, REDIS_URL, RedisError, RedisPrimitiveType

logger = logging.getLogger(__name__)


class AsyncRedisentHelper(RedisentHelper):
    redis_pool: aioredis.ConnectionsPool

    @classmethod
    async def build(cls, redis_url: str = REDIS_URL) -> AsyncRedisentHelper:
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
            raise RedisError(f'Redis Error executing "{op_name}": {ex}', base_exception=ex)
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
                raise RedisError(f'Error setting key "{key}" in Redis: No response')

            return True

    async def get(self, key: str, missing_okay: bool = False, encoding: str = None, use_pool: aioredis.ConnectionsPool = None) -> Optional[RedisPrimitiveType]:
        op_name = f'get(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return await r_conn.get(key, encoding=encoding)

    async def delete(self, key: str, missing_okay: bool = False, use_pool: aioredis.ConnectionsPool = None) -> Optional[bool]:
        op_name = f'delete(key="{key}")'

        if not self.exists(key, use_pool=use_pool):
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
                raise RedisError(f'Error setting hash key "{key}" in Redis: No response')

            return True

    async def hget(self, key: str, name: str, missing_okay: bool = False, encoding: str = None, use_pool: aioredis.ConnectionsPool = None) -> Optional[RedisPrimitiveType]:
        op_name = f'hget(key="{key}", name="{name}")'

        if not self.exists(key, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        if not self.hexists(key, name, use_pool=use_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch entry "{name} in Redis has key "{key}": No such entry')

        async with self.wrapped_redis(op_name, use_pool=use_pool) as r_conn:
            return await r_conn.hget(key, name, encoding=encoding)