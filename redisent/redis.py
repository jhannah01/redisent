import logging
import pickle

from contextlib import asynccontextmanager
from typing import Any, List, Mapping, Optional, Union

import aioredis
import asyncio_redis

from redisent.errors import RedisError
from redisent.utils import RedisPoolConnType, REDIS_URL

logger = logging.getLogger(__name__)

class WrappedRedis:
    redis_url: str

    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_url = redis_url

    async def build_pool(self):
        return await asyncio_redis.Pool.create(self.redis_url)

    @asynccontextmanager
    async def wrapped(self, pool_or_conn=None, operation_name: str = None, close_after: bool = False):
        try:
            pool_or_conn = pool_or_conn or await self.build_pool()
        except Exception as ex:
            raise RedisError(f'Error attempting to connect to "{self.redis_url}": {ex}', base_exception=ex,
                             related_command=operation_name)

        try:
            logger.debug(f'Executing Redis command for "{operation_name}"...')
            yield pool_or_conn
        except Exception as ex:
            logger.exception(f'Encountered Redis Error running "{operation_name}": {ex}')
            raise RedisError(f'Redis Error executing "{operation_name or "Unknown"}": {ex}', base_exception=ex,
                             related_command=operation_name)
        finally:
            if close_after:
                pool_or_conn.close()


@asynccontextmanager
async def wrapped_redis(pool_or_conn: RedisPoolConnType = None, operation_name: str = None, redis_url: str = REDIS_URL):
    try:
        pool_or_conn = pool_or_conn or await aioredis.create_redis_pool(redis_url)
    except Exception as ex:
        raise RedisError(f'Error attempting to connect to "{redis_url}": {ex}', base_exception=ex, related_command=operation_name)

    try:
        logger.debug(f'Executing Redis command for "{operation_name}"...')
        yield pool_or_conn
    except Exception as ex:
        logger.exception(f'Encountered Redis Error running "{operation_name}": {ex}')
        raise RedisError(f'Redis Error executing "{operation_name or "Unknown"}": {ex}', base_exception=ex, related_command=operation_name)
    finally:
        pool_or_conn.close()
        await pool_or_conn.wait_closed()


class RedisHelper:
    @staticmethod
    async def keys(pattern: str = None, encoding: str = 'utf-8', redis_pool: RedisPoolConnType = None) -> List[str]:
        pattern = pattern or '*'

        async with wrapped_redis(redis_pool, operation_name=f'keys("{pattern}")') as redis:
            return await redis.keys(pattern, encoding=encoding)

    @staticmethod
    async def hkeys(key: str, encoding: str = 'utf-8', redis_pool: RedisPoolConnType = None) -> List[str]:
        async with wrapped_redis(redis_pool, operation_name=f'hkeys("{key}")') as redis:
            return await redis.hkeys(key, encoding=encoding)

    @staticmethod
    async def exists(key: str, redis_pool: RedisPoolConnType = None) -> bool:
        async with wrapped_redis(redis_pool, operation_name=f'exists("{key}")') as redis:
            return (await redis.exists(key)) == 1

    @staticmethod
    async def set(key: str, entry: Any, encode_value: bool = True, redis_pool: RedisPoolConnType = None) -> None:
        if encode_value:
            try:
                if not isinstance(entry, bytes):
                    entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error while encoding entry for "{key}" using Pickle: {ex}')

        async with wrapped_redis(redis_pool, f'set("{key}", ...)') as redis:
            await redis.set(key, entry)

    @staticmethod
    async def get(key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, redis_pool: RedisPoolConnType = None):
        if not await RedisHelper.exists(key, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        async with wrapped_redis(redis_pool, operation_name=f'get("{key}")') as redis:
            ent_bytes = await redis.get(key, encoding=None)

        if not decode_value:
            return ent_bytes if not decode_response else ent_bytes.decode('utf-8')

        try:
            return pickle.loads(ent_bytes)
        except Exception as ex:
            raise RedisError(f'Error while decoding Redis entry "{key}" using Pickle: {ex}')

    @staticmethod
    async def delete(key: str, missing_okay: bool = False, redis_pool: RedisPoolConnType = None) -> Optional[bool]:
        if not await RedisHelper.exists(key, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        async with wrapped_redis(redis_pool, operation_name='delete("{key}")') as redis:
            return await redis.delete(key) > 0

    @staticmethod
    async def hdelete(key: str, name: str, missing_okay: bool = False, redis_pool: RedisPoolConnType = None) -> Optional[bool]:
        if not await RedisHelper.hexists(key, name, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry for "{name}" in key "{key}": No such entry in "{key}"')

        async with wrapped_redis(redis_pool, operation_name='hdel("{key}", "{name}")') as redis:
            return await redis.hdel(key, name) > 0

    @staticmethod
    async def hexists(key: str, name: str, redis_pool: RedisPoolConnType = None) -> bool:
        async with wrapped_redis(redis_pool, operation_name=f'hexists("{key}", "{name}")') as redis:
            return await redis.hexists(key, name)

    @staticmethod
    async def hset(key: str, name: str, entry: Any, encode_value: bool = True, redis_pool: RedisPoolConnType = None):
        if encode_value:
            try:
                entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error encoding entry for "{key}" -> "{name}" from Pickle: {ex}')

        async with wrapped_redis(redis_pool, operation_name=f'hset("{key}", "{name}", ...)') as redis:
            await redis.hset(key, name, entry)

    @staticmethod
    async def hgetall(key: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, redis_pool: RedisPoolConnType = None) -> Optional[Mapping[str, Any]]:
        if not await RedisHelper.exists(key, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entry "{key}" (all entries): No such key "{key}" found')

        async with wrapped_redis(redis_pool, operation_name='hgetall("{key}")') as redis:
            entries = await redis.hgetall(key)

            if not decode_value:
                return RedisHelper.convert_keys_to_unicode(entries) if decode_response else entries

            try:
                ent_objs = {ent_name: pickle.loads(ent_value) for ent_name, ent_value in entries.items()}
                return RedisHelper.convert_keys_to_unicode(ent_objs)
            except Exception as ex:
                raise RedisError(f'Error while decoding Redis hash entry "{key}" using Pickle: {ex}', base_exception=ex, extra_attrs={'entries': entries})

    @staticmethod
    async def hget(key: str, name: str, missing_okay: bool = False, decode_value: bool = True, decode_response: bool = True, redis_pool: RedisPoolConnType = None):
        if not await RedisHelper.exists(key, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entry "{key}" -> "{name}": No such key "{key}" found')

        if not await RedisHelper.hexists(key, name, redis_pool=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entry "{key}" -> "{name}": No such field in "{key}" named "{name}" found')

        async with wrapped_redis(redis_pool, operation_name='hget("{key}", "{name}"') as redis:
            entries = await redis.hget(key, name, encoding=None)

            if not decode_value:
                return entries.decode('utf-8') if decode_response and isinstance(entries, bytes) else entries

            try:
                return {ent_name: pickle.loads(ent_value) for ent_name, ent_value in entries.items()}
            except Exception as ex:
                raise RedisError(f'Error while decoding Redis hash entry "{key}" -> "{name}" using Pickle: {ex}', base_exception=ex, extra_attrs={'entries': entries})

    @staticmethod
    def convert_keys_to_unicode(entry: Mapping[Union[str, bytes], Any]):
        return {ent_name.decode('utf-8') if isinstance(ent_name, bytes) else ent_name: ent_value for ent_name, ent_value in entry.items()}