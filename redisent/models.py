from __future__ import annotations

import logging

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, Union, List, Optional

from redisent import RedisError
from redisent.redis import RedisHelper
from redisent.utils import RedisPoolConnType

logger = logging.getLogger(__name__)


@dataclass
class RedisEntry:
    redis_id: str = field(default_factory=str)

    @property
    def entry_fields(self) -> List[str]:
        return self.get_entry_fields()

    @classmethod
    def get_entry_fields(cls) -> List[str]:
        return [fld.name for fld in fields(cls)]

    def __new__(cls, *args, **kwargs):
        if not is_dataclass(cls):
            raise NotImplementedError(f'All instances of "{cls.__name__}" must be decorated with @dataclass')

        return super().__new__(cls)

    def __init__(self, redis_id: str, *args, **kwargs) -> None:
        self.redis_id = redis_id

    def to_dict(self, excluded_attributes: List[str] = None) -> Mapping[str, Any]:
        excluded_attributes = excluded_attributes or []
        return {attr: val for attr, val in asdict(self).items() if not attr.startswith('_') and attr not in excluded_attributes}

    @classmethod
    def from_dict(cls, entry_dict: Mapping[str, Any]) -> RedisEntry:
        ent_kwargs = {}
        flds = [fld.name for fld in fields(cls)]

        for fld, val in entry_dict.items():
            if fld not in flds:
                logger.debug(f'Found unexpected field "{fld}" in dictionary. Ignoring (value: "{val}")')
                continue

            ent_kwargs[fld] = val

        return cls(**ent_kwargs)

    @classmethod
    async def from_redis(cls, redis_id: str, missing_okay: bool = False, decode_value: bool = True, redis_pool: RedisPoolConnType = None) -> Optional[RedisEntry]:
        if not await RedisHelper.exists(redis_id, pool_or_conn=redis_pool):
            if missing_okay:
                return None

            raise RedisError(f'Unable to find Redis entry for "{redis_id}"')

        ent_dict = await RedisHelper.get(redis_id, missing_okay=missing_okay, decode_value=decode_value, pool_or_conn=redis_pool)

        cls_kwargs = {}

        for attr_name, attr_value in ent_dict.items():
            if attr_name.startswith('_') or attr_name not in cls.get_entry_fields():
                logger.debug(f'Skipping internal field / missing field "{attr_name}" (value: {attr_value}). No such field in "{cls.__name__}"')
                continue

            cls_kwargs[attr_name] = attr_value

        return cls(redis_id=redis_id, **cls_kwargs)

    async def to_redis(self, use_redis_id: str = None, redis_pool: RedisPoolConnType = None) -> None:
        if use_redis_id:
            self.redis_id = use_redis_id

        if await RedisHelper.exists(self.redis_id, pool_or_conn=redis_pool):
            print(f'Overwriting Redis entry for "{self.redis_id}" in Redis (already exists)')

        await RedisHelper.set(self.redis_id, self.to_dict(), encode_value=True, pool_or_conn=redis_pool)


@dataclass
class RedisHashEntry(RedisEntry):
    redis_name: str = field(default_factory=str)

    def __init__(self, redis_id: str, redis_name: str, *args, **kwargs) -> None:
        super(RedisHashEntry, self).__init__(redis_id, *args, **kwargs)
        self.redis_name = redis_name

    @classmethod
    async def from_redis(cls, redis_id: str, redis_name: str = None, encode_value: bool = True, redis_pool: RedisPoolConnType = None) -> Mapping[str, RedisHashEntry]:
        if redis_name:
            if ':' in redis_name:
                (l_val, r_val) = redis_name.split(':')

                if l_val == '*' and r_val == '*':
                    raise RedisError(f'Unable to look up Redis entries for "{redis_id}" with pattern "{redis_name}": Both sides are wildcards')

                query_val = l_val if r_val == '*' else r_val
                redis_entries = await RedisHelper.hgetall(redis_id, decode_value=encode_value, redis_pool=redis_pool)
                entries = {ent_key: env_value for ent_key, env_value in redis_entries.items() if query_val == ent_key}
            else:
                if not await RedisHelper.hexists(redis_id, redis_name):
                    raise RedisError(f'Unable to find Redis entry under "{redis_id}" for key "{redis_name}"')

                entries = await RedisHelper.hget(redis_id, redis_name, decode_value=encode_value, redis_pool=redis_pool)
        else:
            entries = await RedisHelper.hgetall(redis_id, redis_pool=redis_pool)

        if not entries:
            logger.debug(f'Found no matches in redis for "{query_val}" of "{redis_id}" using "{redis_name}"')
            return {}

        entry_fields = [fld.name for fld in fields(cls)]

        for ent_name, ent_dict in entries.items():
            cls_kwargs = {}

            for attr_name, attr_value in ent_dict.items():
                if attr_name.startswith('_') or attr_name not in entry_fields:
                    logger.debug(f'Skipping internal field / missing field "{attr_name}" (value: {attr_value}). No such field in "{cls.__name__}"')
                    continue

                cls_kwargs[attr_name] = attr_value

            entries[ent_name] = cls(redis_id=redis_id, redis_name=redis_name, **cls_kwargs)

        return entries

    async def to_redis(self, use_redis_id: str = None, use_redis_name: str = None, redis_pool: RedisPoolConnType = None) -> None:
        redis_id = use_redis_id or self.redis_id
        redis_name = use_redis_name or self.redis_name

        if await RedisHelper.hexists(redis_id, redis_name, redis_pool=redis_pool):
            print(f'Overwriting Redis entry for in "{self.redis_id}" for "{redis_name}" in Redis (already exists)')

        await RedisHelper.hset(redis_id, redis_name, self.to_dict(), encode_value=True, redis_pool=redis_pool)