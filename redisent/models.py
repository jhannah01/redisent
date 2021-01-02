from __future__ import annotations

import logging
import pickle

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, List, Optional, Union

from redisent.helpers import BlockingRedisentHelper, AsyncRedisentHelper
from redisent import RedisError

logger = logging.getLogger(__name__)


@dataclass()
class RedisEntry:
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

    def __init__(self, *args, **kwargs) -> None:
        pass

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
    def from_dict(cls, redis_id: str, redis_name: str = None, **ent_kwargs) -> RedisEntry:
        if not redis_name:
            if 'redis_name' in ent_kwargs:
                redis_name = ent_kwargs.pop('redis_name')

        cls_kwargs = {attr: ent_kwargs[attr] for attr in cls.get_entry_fields(include_redis_fields=False, include_internal_fields=False) if attr in ent_kwargs}
        # noinspection PyArgumentList

        cls_kwargs['redis_id'] = redis_id
        if redis_name:
            cls_kwargs['redis_name'] = redis_name

        return cls(**cls_kwargs)

    def as_dict(self, include_redis_attrs: bool = True, include_internal_attrs: bool = False) -> Mapping[str, Any]:
        ent_dict = asdict(self)

        if include_redis_attrs and include_internal_attrs:
            return ent_dict

        flds = self.get_entry_fields(include_redis_fields=include_redis_attrs, include_internal_fields=include_internal_attrs)
        return {attr: value for attr, value in ent_dict.items() if attr in flds}

    @classmethod
    def decode_entry(cls, entry_bytes) -> RedisEntry:
        try:
            ent = pickle.loads(entry_bytes)

            if isinstance(ent, Mapping):
                return cls.from_dict(**ent)
            elif not isinstance(ent, RedisEntry):
                raise RedisError('Decoded entry is neither a dictionary nor a Mapping')

            return ent
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

    @classmethod
    def fetch(cls, helper: BlockingRedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        entry_bytes = helper.hget(redis_id, redis_name, return_bytes=True) if redis_name else helper.get(redis_id)
        if not entry_bytes:
            name_str = f' of entry "{redis_name}"' if redis_name else ''
            raise RedisError(f'Failure during fetch of key "{redis_id}"{name_str}: No data returned')

        return cls.decode_entry(entry_bytes)

    @classmethod
    async def async_fetch(cls, helper: AsyncRedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        entry_bytes = await (helper.hget(redis_id, redis_name) if redis_name else helper.get(redis_id))
        if not entry_bytes:
            name_str = f' of entry "{redis_name}"' if redis_name else ''
            raise RedisError(f'Failure during async fetch of key "{redis_id}"{name_str}: No data returned')
        return cls.decode_entry(entry_bytes)

    def store(self, helper: BlockingRedisentHelper) -> bool:
        entry_bytes = self.encode_entry(self)
        return helper.hset(self.redis_id, self.redis_name, entry_bytes) if self.redis_name else helper.set(self.redis_id, entry_bytes)

    async def async_store(self, helper: AsyncRedisentHelper) -> bool:
        entry_bytes = self.encode_entry(self)
        if self.redis_name:
            return await helper.hset(self.redis_id, self.redis_name, entry_bytes)

        return await helper.set(self.redis_id, entry_bytes)