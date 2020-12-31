from __future__ import annotations

import logging
import pickle

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, Union, List, Optional, Dict

from redisent import RedisError
from redisent.redishelper import AsyncRedisHelper
from redisent.utils import RedisPoolConnType

logger = logging.getLogger(__name__)


@dataclass()
class RedisEntry:
    helper: AsyncRedisHelper = field(repr=False, compare=False, hash=False, metadata={'redis_field': True, 'internal': True})
    redis_id: str = field(metadata={'redis_field': True})

    redis_name: Optional[str] = field(default_factory=str, metadata={'redis_field': True})

    def dump(self) -> str:
        dump_out = f'RedisEntry ({type(self).__name__}) for key "{self.redis_id}"'

        if self.redis_name:
            dump_out = f'{dump_out}, hash entry "{self.redis_name}":'

        for attr in self.get_entry_fields(include_redis_fields=False, include_internal_fields=False):
            dump_out = f'{dump_out}\n=> {attr}\t-> "{getattr(self, attr)}"'

        return dump_out

    @classmethod
    def get_entry_fields(cls, include_redis_fields: bool = False, include_internal_fields: bool = False) -> List[str]:
        flds = []

        for fld in fields(cls):
            is_redis_fld = fld.metadata.get('redis_field', False)
            is_int_fld = fld.metadata.get('internal_field', False)

            if is_redis_fld and not include_redis_fields:
                continue

            if is_int_fld and not include_internal_fields:
                continue

            flds.append(fld.name)

        return flds

    @property
    def entry_fields(self) -> List[str]:
        return self.get_entry_fields(include_redis_fields=False, include_internal_fields=False)

    @property
    def is_hashmap(self) -> bool:
        return True if self.redis_name else False

    def __new__(cls, *args, **kwargs):
        if not is_dataclass(cls):
            raise NotImplementedError(f'All instances of "{cls.__name__}" must be decorated with @dataclass')

        return super().__new__(cls)

    @classmethod
    def from_dict(cls, helper: AsyncRedisHelper, redis_id: str, redis_name: str = None, **ent_kwargs) -> RedisEntry:
        if not redis_name:
            if 'redis_name' in ent_kwargs:
                redis_name = ent_kwargs.pop('redis_name')

        cls_kwargs = {attr: ent_kwargs[attr] for attr in cls.get_entry_fields(include_redis_fields=False, include_internal_fields=False) if attr in ent_kwargs}
        # noinspection PyArgumentList
        return cls(helper=helper, redis_id=redis_id, redis_name=redis_name, **cls_kwargs)

    def as_dict(self, include_redis_attrs: bool = True, include_internal_attrs: bool = False) -> Mapping[str, Any]:
        ent_dict = asdict(self)

        if include_redis_attrs and include_internal_attrs:
            return ent_dict

        flds = self.get_entry_fields(include_redis_fields=include_redis_attrs, include_internal_fields=include_internal_attrs)
        return {attr: value for attr, value in ent_dict.items() if attr in flds}

    @classmethod
    def fetch(cls, helper: AsyncRedisHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        if not helper.redis.exists(redis_id):
            raise KeyError(f'No entry found for Redis key "{redis_id}"')

        if redis_name:
            if not helper.redis.hexists(redis_id, redis_name):
                raise KeyError(f'No hash entry found for Redis key "{redis_id}" for entry "{redis_name}"')

            ent_bytes = helper.redis.hget(redis_id, redis_name)
        else:
            ent_bytes = helper.redis.get(redis_id)

        if not ent_bytes:
            ent_str = f' (entry name: "{redis_name}")' if redis_name else ''
            raise RedisError(f'No entry data returned from Redis for key "{redis_id}"{ent_str}')

        return RedisEntry.decode_entry(helper, ent_bytes)

    def store(self, helper: AsyncRedisHelper) -> bool:
        ent_bytes = RedisEntry.encode_entry(self, as_mapping=self.is_hashmap)

        if self.is_hashmap:
            res = helper.redis.hset(self.redis_id, self.redis_name, ent_bytes)
        else:
            res = helper.redis.set(self.redis_id, ent_bytes)

        return True if res and res > 0 else False

    @classmethod
    def decode_entry(cls, helper: AsyncRedisHelper, entry_bytes: bytes) -> RedisEntry:
        try:
            ent = pickle.loads(entry_bytes)

            if isinstance(ent, RedisEntry):
                return ent

            return cls.from_dict(helper, **ent)

        except Exception as ex:
            err_message = f'Error decoding entry using pickle: {ex}'
            logger.exception(err_message)
            raise Exception(err_message)

    @classmethod
    def encode_entry(cls, entry: RedisEntry, as_mapping: bool = None) -> bytes:
        if as_mapping is None:
            as_mapping = True if entry.redis_name else False

        try:
            return pickle.dumps(entry.as_dict(include_redis_attrs=True, include_internal_attrs=False) if as_mapping is True else entry)
        except Exception as ex:
            ent_str = f' (entry name: "{entry.redis_name}")' if entry.redis_name else ''
            raise Exception(f'Error encoding entry for "{entry.redis_id}"{ent_str} using pickle: {ex}')
