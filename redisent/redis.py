import pickle
from typing import Any, List, Mapping, Optional

from redisent.errors import RedisError
from redisent.utils import RedisPoolConnType, wrapped_redis


class RedisHelper:
    @staticmethod
    async def keys(pattern: str = None, pool_or_conn: RedisPoolConnType = None, encoding: str = 'utf-8') -> List[str]:
        pattern = pattern or '*'

        async with wrapped_redis(pool_or_conn, operation_name=f'keys("{pattern}")') as redis:
            return await redis.keys(pattern, encoding=encoding)

    @staticmethod
    async def hkeys(key: str, pool_or_conn: RedisPoolConnType = None, encoding: str = 'utf-8') -> List[str]:
        async with wrapped_redis(pool_or_conn, operation_name=f'hkeys("{key}")') as redis:
            return await redis.hkeys(key, encoding=encoding)

    @staticmethod
    async def exists(key: str, pool_or_conn: RedisPoolConnType = None) -> bool:
        async with wrapped_redis(pool_or_conn, operation_name=f'exists("{key}")') as redis:
            return (await redis.exists(key)) == 1

    @staticmethod
    async def set(key: str, entry: Any, encode_value: bool = True, pool_or_conn: RedisPoolConnType = None) -> None:
        if encode_value:
            try:
                if not isinstance(entry, bytes):
                    entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error while encoding entry for "{key}" using Pickle: {ex}')

        async with wrapped_redis(pool_or_conn, f'set("{key}", ...)') as redis:
            await redis.set(key, entry)

    @staticmethod
    async def get(key: str, missing_okay: bool = False, decode_value: bool = True, pool_or_conn: RedisPoolConnType = None) -> Optional[Mapping[str, Any]]:
        if not await RedisHelper.exists(key, pool_or_conn=pool_or_conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis entry "{key}": Does not exist')

        async with wrapped_redis(pool_or_conn, operation_name=f'get("{key}")') as redis:
            ent_bytes = await redis.get(key, encoding=None)

        if not decode_value:
            return ent_bytes

        try:
            return pickle.loads(ent_bytes)
        except Exception as ex:
            raise RedisError(f'Error while decoding Redis entry "{key}" using Pickle: {ex}')

    @staticmethod
    async def delete(key: str, missing_okay: bool = False, pool_or_conn: RedisPoolConnType = None) -> Optional[bool]:
        if not await RedisHelper.exists(key, pool_or_conn=pool_or_conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry "{key}": Does not exist')

        async with wrapped_redis(pool_or_conn, operation_name='delete("{key}")') as redis:
            return await redis.delete(key) > 0

    @staticmethod
    async def hdelete(key: str, name: str, missing_okay: bool = False, pool_or_conn: RedisPoolConnType = None) -> Optional[bool]:
        if not await RedisHelper.hexists(key, name, pool_or_conn=pool_or_conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to delete Redis entry for "{name}" in key "{key}": No such entry in "{key}"')

        async with wrapped_redis(pool_or_conn, operation_name='hdel("{key}", "{name}")') as redis:
            return await redis.hdel(key, name) > 0

    @staticmethod
    async def hexists(key: str, name: str, pool_or_conn: RedisPoolConnType = None) -> bool:
        async with wrapped_redis(pool_or_conn, operation_name=f'hexists("{key}", "{name}")') as redis:
            return await redis.hexists(key, name)

    @staticmethod
    async def hset(key: str, name: str, entry: Any, encode_value: bool = True, pool_or_conn: RedisPoolConnType = None):
        if encode_value:
            try:
                entry = pickle.dumps(entry)
            except Exception as ex:
                raise RedisError(f'Error encoding entry for "{key}" -> "{name}" from Pickle: {ex}')

        async with wrapped_redis(pool_or_conn, operation_name=f'hset("{key}", "{name}", ...)') as redis:
            await redis.hset(key, name, entry)

    @staticmethod
    async def hget(key: str, name: str = None, missing_okay: bool = False, decode_value: bool = True, pool_or_conn: RedisPoolConnType = None):
        if not await RedisHelper.exists(key, pool_or_conn=pool_or_conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entry "{key}" -> "{name}": No such key "{key}" found')

        if not name:
            async with wrapped_redis(pool_or_conn, operation_name=f'hgetall("{key}")') as redis:
                entries = await redis.hgetall(key)

            if not decode_value:
                return entries

            return {ent_name: pickle.loads(ent_value) for ent_name, ent_value in entries.items()}

        if not await RedisHelper.hexists(key, name, pool_or_conn=pool_or_conn):
            if missing_okay:
                return None

            raise RedisError(f'Unable to fetch Redis hash entry "{key}" -> "{name}": No such field in "{key}" named "{name}" found')

        async with wrapped_redis(pool_or_conn, operation_name=f'hget("{key}", "{name}")') as redis:
            ent_bytes = await redis.hget(key, name, encoding=None)

        if not decode_value:
            return ent_bytes

        try:
            return pickle.loads(ent_bytes)
        except Exception as ex:
            raise RedisError(f'Error while decoding Redis hash entry "{key}" -> "{name}" using Pickle: {ex}')
