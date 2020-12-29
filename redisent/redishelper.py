import logging
import pickle

from contextlib import asynccontextmanager
from typing import Any, List, Mapping, Optional, Union

import aioredis
import asyncio_redis
from asyncio_redis import Pool
# from decorator import decorator

from redisent.errors import RedisError
from redisent.utils import RedisPoolConnType, REDIS_URL

logger = logging.getLogger(__name__)

'''
async def wrapped_redis(redis_pool: Pool, operation_name: str = None, close_after: bool = True):
    async def _wrapped(func, *args, **kwargs):
        try:
            async with redis_pool as redis_conn:
                try:
                    logger.debug(f'Executing Redis command for "{operation_name}"...')
                    return func(*args, redis_conn=redis_conn, **kwargs)
                except Exception as ex:
                    logger.exception(f'Encountered Redis Error running "{operation_name}": {ex}')
                    raise RedisError(f'Redis Error executing "{operation_name or "Unknown"}": {ex}', base_exception=ex,
                                     related_command=operation_name)
        finally:
            if close_after:
                redis_pool.close()

    return _wrapped

'''


class RedisHelper:
    redis_pool: RedisPoolConnType
    redis_url: str

    def __init__(self, redis_pool: RedisPoolConnType):
        self.redis_pool = redis_pool

    def __del__(self):
        if self.redis_pool:
            logger.info('Cleaning up Redis pool on deletion')

            self.redis_pool.close()

    @classmethod
    async def build(cls, redis_pool: RedisPoolConnType = None, redis_url: str = REDIS_URL):
        redis_url = redis_url or REDIS_URL
        redis_pool = redis_pool or await cls.build_pool(redis_url)
        return RedisHelper(redis_pool)

    @classmethod
    async def build_pool(cls, redis_url: str = REDIS_URL):
        try:
            return await aioredis.create_redis_pool(redis_url)
        except Exception as ex:
            raise RedisError(f'Error attempting to connect to "{redis_url}": {ex}', base_exception=ex)

    @asynccontextmanager
    async def wrapped_redis(self, operation_name: str = None, pool: Pool = None, close_after: bool = False):
        operation_name = operation_name or "Unknown"
        pool = pool or self.redis_pool

        try:
            logger.debug(f'Executing Redis command for "{operation_name}"...')
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

    async def keys(self, pattern: str = None, encoding: str = 'utf-8', pool: Pool = None) -> List[str]:
        pattern = pattern or '*'

        async with self.wrapped_redis(operation_name=f'keys("{pattern}")', pool=pool) as redis_conn:
            return await redis_conn.keys(pattern, encoding=encoding)

    async def hkeys(self, key: str, encoding: str = 'utf-8', pool: Pool = None) -> List[str]:
        async with self.wrapped_redis(operation_name=f'hkeys("{key}")', pool=pool) as redis_conn:
            return await redis_conn.hkeys(key, encoding=encoding)

    async def exists(self, key: str, pool: Pool = None) -> bool:
        async with self.wrapped_redis(operation_name=f'exists("{key}")', pool=pool) as redis_conn:
            return (await redis_conn.exists(key)) == 1

    async def hexists(self, key: str, name: str, pool: RedisPoolConnType = None) -> bool:
        async with self.wrapped_redis(operation_name=f'hexists("{key}", "{name}")', pool=pool) as redis_conn:
            return await redis_conn.hexists(key, name)

    async def set(self, key: str, entry: Any, encode_value: bool = True, pool: RedisPoolConnType = None) -> bool:
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

    async def hset(self, key: str, name: str, entry: Any, encode_value: bool = True, pool: RedisPoolConnType = None) -> bool:
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

    async def get(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, pool: RedisPoolConnType = None):
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

    async def hget(self, key: str, name: str, missing_okay: bool = False, decode_value: bool = True, pool: RedisPoolConnType = None):
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

    async def delete(self, key: str, missing_okay: bool = False, pool: RedisPoolConnType = None) -> Optional[bool]:
        if not await self.exists(key, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        async with self.wrapped_redis(pool=pool, operation_name='delete("{key}")') as redis:
            return await redis.delete(key) > 0

    async def hdelete(self, key: str, name: str, missing_okay: bool = False, pool: RedisPoolConnType = None) -> Optional[bool]:
        if not await self.hexists(key, name, pool=pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry for "{name}" in key "{key}": No such entry in "{key}"')

        async with self.wrapped_redis(pool=pool, operation_name='hdel("{key}", "{name}")') as redis:
            return await redis.hdel(key, name) > 0

    async def hgetall(self, key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, pool: RedisPoolConnType = None) -> Optional[Mapping[str, Any]]:
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
                return RedisHelper.convert_keys_to_unicode(ent_objs)
            except Exception as ex:
                raise RedisError(f'Error while decoding Redis hash entry "{key}" using Pickle: {ex}', base_exception=ex, extra_attrs={'entries': entries})


    @staticmethod
    def convert_keys_to_unicode(entry: Mapping[Union[str, bytes], Any]):
        return {ent_name.decode('utf-8') if isinstance(ent_name, bytes) else ent_name: ent_value for ent_name, ent_value in entry.items()}

