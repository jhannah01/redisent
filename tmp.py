from __future__ import annotations

import aioredis
import pickle

from contextlib import asynccontextmanager
from dataclasses import dataclass, fields, field, asdict
from typing import Mapping, Any, Optional, Union, List


from redisent.model import REDIS_URL

RedisPoolConnType = Union[aioredis.Redis, aioredis.ConnectionsPool, aioredis.RedisConnection]


class RedisError(Exception):
    pass


@asynccontextmanager
async def wrapped_redis(pool_or_conn: RedisPoolConnType = None, operation_name: str = None):
    try:
        pool_or_conn = pool_or_conn or await aioredis.create_redis_pool(REDIS_URL)
    except Exception as ex:
        raise RedisError(f'Error attempting to connect to "{REDIS_URL}": {ex}')

    try:
        yield pool_or_conn
    except Exception as ex:
        raise RedisError(f'Redis Error executing "{operation_name or "Unknown"}": {ex}')
    finally:
        pool_or_conn.close()
        await pool_or_conn.wait_closed()


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


class RedisEntry:
    redis_id: str
    store_encoded: bool

    fields: Mapping[str, Any] = {}

    def __init__(self, redis_id: str, store_encoded: bool = True, entry_fields: Mapping[str, Any] = None):
        self.redis_id = redis_id
        self.store_encoded = store_encoded

        if fields:
            self.fields = entry_fields

    @classmethod
    async def load(cls, redis_id: str, is_encoded: bool = True, redis_pool: RedisPoolConnType = None):
        if not await RedisHelper.exists(redis_id, pool_or_conn=redis_pool):
            raise RedisError(f'No entry with ID "{redis_id}" found in Redis')

        ent_redis = await RedisHelper.get(redis_id, decode_value=is_encoded, pool_or_conn=redis_pool)

        if not is_encoded:
            return ent_redis

        return cls(redis_id, store_encoded=is_encoded, entry_fields=ent_redis)

    async def save(self, redis_pool: RedisPoolConnType = None):
        if await RedisHelper.exists(self.redis_id, pool_or_conn=redis_pool):
            print(f'Overwriting entry for "{self.redis_id}" in Redis (already exists)')

        await RedisHelper.set(self.redis_id, self.fields, encode_value=self.store_encoded, pool_or_conn=redis_pool)


@dataclass
class Reminder:
    member_id: str = field()
    trigger_ts: float = field()

    created_ts: float = field()
    member_name: str = field()
    channel_id: str = field()
    channel_name: str = field()
    provided_when: str = field()
    content: str = field()

    is_complete: bool = field(default=False)

    @property
    def redis_id(self) -> str:
        return f'{self.member_id}:{self.trigger_ts}'

    @classmethod
    def from_dict(cls, entry_dict: Mapping[str, Any]) -> Reminder:
        ent_kwargs = {}
        flds = [fld.name for fld in fields(cls)]

        for fld, val in entry_dict.items():
            if fld not in flds:
                # print(f'Warning: Found unexpected field "{fld}" in dictionary. Ignoring (value: "{val}")')
                continue

            ent_kwargs[fld] = val

        return Reminder(**ent_kwargs)

    def as_dict(self) -> Mapping[str, Any]:
        return asdict(self)

    @classmethod
    async def from_redis(cls, member_id: str, trigger_ts: float = None) -> Union[Reminder, Mapping[str, Reminder]]:
        if trigger_ts:
            redis_id = f'{member_id}:{trigger_ts}'
            if not await RedisHelper.hexists('reminders', redis_id):
                raise Exception(f'Unable to find reminder ID "{redis_id}" in Redis')

            ent_dict = await RedisHelper.hget('reminders', redis_id, decode_value=True)
            return Reminder.from_dict(ent_dict)

        reminders = {}
        rem_entries = await RedisHelper.hget('reminders') or {}

        for red_id, red_ent in rem_entries.items():
            red_id = red_id.decode('utf-8')

            mem, trg = red_id.split(':')
            if mem != member_id:
                continue

            reminders[red_id] = Reminder.from_dict(red_ent)

        return reminders
