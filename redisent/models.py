from __future__ import annotations

import asyncio
import logging
import pickle

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, List, Optional, Union

import redis

from redisent.helpers import RedisentHelper
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
    def decode_entry(cls, entry_bytes, use_redis_id: str = None, use_redis_name: str = None) -> RedisEntry:
        try:
            ent = pickle.loads(entry_bytes)

            if isinstance(ent, Mapping):
                redis_id = ent.pop('redis_id', None)
                redis_id = use_redis_id or redis_id

                if not redis_id:
                    raise RedisError('Unable to convert dictionary from Redis into RedisEntry (no value for "redis_id" found)')

                redis_name = ent.pop('redis_name', None)
                redis_name = use_redis_name or redis_name

                return cls.from_dict(redis_id, redis_name=redis_name, **ent)
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
    def fetch(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        op_name = f'get(key="{redis_id}")' if not redis_name else f'hget(key="{redis_id}", name="{redis_name}")'
        name_str = f' of entry "{redis_name}"' if redis_name else ''

        if helper.use_async:
            raise ValueError(f'Cannot use async fetch helper for blocking fetch request of "{redis_id}"{name_str}')

        with helper.wrapped_redis(op_name=op_name) as r_conn:
            entry_bytes = r_conn.get(redis_id) if not redis_name else r_conn.hget(redis_id, redis_name)

        if not entry_bytes:
            raise RedisError(f'Failure during fetch of key "{redis_id}"{name_str}: No data returned')

        return cls.decode_entry(entry_bytes)

    @classmethod
    async def fetch_async(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        op_name = f'get(key="{redis_id}")' if not redis_name else f'hget(key="{redis_id}", name="{redis_name}")'
        name_str = f' of entry "{redis_name}"' if redis_name else ''

        if not helper.use_async:
            raise ValueError(f'Cannot use blocking fetch helper for async fetch request of "{redis_id}"{name_str}')

        async with helper.wrapped_redis_async(op_name=op_name) as r_conn:
            coro = r_conn.get(redis_id, encoding=None) if not redis_name else r_conn.hget(redis_id, redis_name, encoding=None)
            entry_bytes = await coro

        if not entry_bytes:
            raise RedisError(f'Failure during fetch of key "{redis_id}"{name_str}: No data returned')

        return cls.decode_entry(entry_bytes)

    def store(self, helper: RedisentHelper) -> bool:
        entry_bytes = self.encode_entry(self)
        op_name = f'set(key="{self.redis_id}")' if not self.redis_name else f'hset(key="{self.redis_id}", name="{self.redis_name}")'

        if not helper.use_async:
            with helper.wrapped_redis(op_name=op_name) as r_conn:
                return r_conn.set(self.redis_id, entry_bytes) if not self.redis_name else r_conn.hset(self.redis_id, self.redis_name, entry_bytes)
        else:
            async def store_async():
                async with helper.wrapped_redis_async(op_name=op_name) as r_conn_async:
                    if self.redis_name:
                        res = await r_conn_async.hset(self.redis_id, self.redis_name, entry_bytes)
                    else:
                        res = await r_conn_async.set(self.redis_id, entry_bytes)

                    return res

            return helper.async_loop.run_until_complete(store_async())