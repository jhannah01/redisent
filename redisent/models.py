from __future__ import annotations

import logging
import pickle

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, Union, List, Optional

from redisent import RedisError
from redisent.redis import RedisHelper
from redisent.utils import RedisPoolConnType

logger = logging.getLogger(__name__)


@dataclass
class RedisEntry:
    redis_id: str
    redis_name: Optional[str] = field(default=None)
    store_encoded: bool = field(default=True)
    is_hashmap: bool = field(default=True)


    def __new__(cls, *args, **kwargs):
        if not is_dataclass(cls):
            raise NotImplementedError(f'All instances of "{cls.__name__}" must be decorated with @dataclass')

        return super(RedisEntry, cls).__new__(cls)

    @classmethod
    def encode_entry(cls, entry: RedisEntry) -> bytes:
        try:
            return pickle.dumps(entry)
        except pickle.PickleError as ex:
            raise RedisError(f'Error encoding entry "{entry.redis_id}" with Pickle: {ex}', base_exception=ex, extra_attrs={'entry': entry})

    @classmethod
    def decode_entry(cls, redis_id: str, entry_bytes: bytes, redis_name: str = None) -> Mapping[str, Any]:
        try:
            return pickle.loads(entry_bytes)
        except pickle.PickleError as ex:
            red_name = f' (entry: "{redis_name}")'
            raise RedisError(f'Error decoding entry from Redis for "{redis_id}"{red_name}: {ex}', base_exception=ex,
                             extra_attrs={'redis_id': redis_id, 'redis_name': redis_name, 'entry_bytes': entry_bytes})

    def to_dict(self, override_attributes: Mapping[str, Any] = None) -> Mapping[str, Any]:
        entry_dict = {attr: val for attr, val in asdict(self).items() if not attr.startswith('_')}

        if override_attributes:
            entry_dict.update(override_attributes)

        return entry_dict

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [fld.name for fld in fields(cls)]

    @property
    def field_names(self) -> List[str]:
        return self.get_field_names()

    @classmethod
    def from_dict(cls, entry_dict: Mapping[str, Any]) -> RedisEntry:
        ent_kwargs = {}
        flds = cls.field_names

        for fld, val in entry_dict.items():
            if fld not in flds:
                logger.debug(f'Found unexpected field "{fld}" in dictionary. Ignoring (value: "{val}")')
                continue

            ent_kwargs[fld] = val

        return cls(**ent_kwargs)

    @classmethod
    async def from_redis(cls, helper: RedisHelper, redis_id: str, redis_name: str = None, is_hashmap: bool = True, missing_okay: bool = False, is_encoded: bool = True) -> Optional[RedisEntry]:
        cls_kwargs = {}
        fld_names = cls.get_field_names()

        if not is_hashmap:
            ent_dict = await helper.get(redis_id, missing_okay=missing_okay, decode_value=is_encoded)
        else:
            if not redis_name:
                raise RedisError(f'Unable to fetching hash map entry from key "{redis_id}" without a provided entry name')

            ent_dict = await helper.hget(redis_id, name=redis_name, missing_okay=missing_okay, decode_value=is_encoded)

        if is_hashmap:
            for attr_name, attr_value in ent_dict.items():
                if attr_name.startswith('_') or attr_name not in fld_names:
                    logger.debug(f'Skipping internal field / missing field "{attr_name}" (value: {attr_value}). No such field in "{cls.__name__}"')
                    continue

                cls_kwargs[attr_name] = attr_value
        else:
            cls_kwargs = ent_dict

        return cls(**cls_kwargs)

    async def to_redis(self, helper: RedisHelper, use_redis_id: str = None) -> None:
        if use_redis_id:
            self.redis_id = use_redis_id

        if await helper.exists(self.redis_id):
            print(f'Overwriting Redis entry for "{self.redis_id}" in Redis (already exists)')

        await helper.set(self.redis_id, self.to_dict(), encode_value=True)


@dataclass
class RedisHashEntry(RedisEntry):
    redis_name: str = field(default_factory=str)

    def __init__(self, redis_id: str, redis_name: str, *args, **kwargs) -> None:
        super(RedisHashEntry, self).__init__(redis_id, *args, **kwargs)
        self.redis_name = redis_name

    @classmethod
    async def from_redis(cls, helper: RedisHelper, redis_id: str, redis_name: str = None, decode_value: bool = True) -> Mapping[str, RedisHashEntry]:

        if not redis_name:
            redis_entires = await helper.hgetall(redis_id, decode_value=decode_value, pool=pool)
        else:
            if ':' not in redis_name:
                (l_val, r_val) = redis_name.split(':')

                if l_val == '*' and r_val == '*':
                    raise RedisError(f'Unable to look up Redis entries for "{redis_id}" with pattern "{redis_name}": Both sides are wildcards')

                query_val = l_val if r_val == '*' else r_val
                redis_entries = await helper.hgetall(redis_id, decode_value=decode_value, pool=pool)

                if not redis_entries:
                    return {}

            if not await helper.hexists(redis_id, redis_name, pool=pool):
                raise RedisError(f'Unable to find Redis entry under "{redis_id}" for key "{redis_name}"')

            entries = await helper.hget(redis_id, redis_name, decode_value=decode_value, pool=pool)
            entries = {ent_key: env_value for ent_key, env_value in redis_entries.items() if query_val == ent_key}
            entries = await helper.hgetall(redis_id, pool=pool)

        if not entries:
            logger.debug(f'No entries found for "{redis_name}" in hash key "{redis_id}"')
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

    async def to_redis(self, helper: RedisHelper, use_redis_id: str = None, use_redis_name: str = None, pool: RedisPoolConnType = None) -> None:
        redis_id = use_redis_id or self.redis_id
        redis_name = use_redis_name or self.redis_name

        if await helper.hexists(redis_id, redis_name, pool=pool):
            print(f'Overwriting Redis entry for in "{self.redis_id}" for "{redis_name}" in Redis (already exists)')

        await helper.hset(redis_id, redis_name, self.to_dict(), encode_value=True, pool=pool)
