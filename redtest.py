from __future__ import annotations

import pickle
from datetime import datetime

from redis import StrictRedis
from typing import Mapping, Any, List, Optional, Union, Dict
from dataclasses import dataclass, field, fields, asdict, is_dataclass


class RedisHelper:
    redis: redis.StrictRedis

    def __init__(self, redis_conn: StrictRedis = None) -> None:
        self.redis = redis_conn or StrictRedis('rpi04.synistree.com', decode_responses=False)


@dataclass()
class RedisEntry:
    redis_id: str = field(metadata={'redis_field': True})
    store_as_mapping: bool = field(default=False, metadata={'redis_field': True})

    redis_name: Optional[str] = field(default=None, metadata={'redis_field': True})

    @classmethod
    def get_entry_fields(cls, include_all_fields: bool = False) -> List[str]:
        return [fld.name for fld in fields(cls) if include_all_fields or not fld.metadata.get('redis_field', False)]

    @property
    def entry_fields(self) -> List[str]:
        return self.get_entry_fields(include_all_fields=False)

    @property
    def is_hashmap(self) -> bool:
        return True if self.redis_name else False

    def __new__(cls, *args, **kwargs):
        if not is_dataclass(cls):
            raise NotImplementedError(f'All instances of "{cls.__name__}" must be decorated with @dataclass')

        return super().__new__(cls)

    @classmethod
    def from_dict(cls, redis_id: str = None, redis_name: str = None, **ent_kwargs) -> RedisEntry:
        if not redis_id:
            if 'redis_id' not in ent_kwargs:
                raise ValueError(f'Cannot build {cls.__name__} instance from dictionary without "redis_id" value')

            redis_id = ent_kwargs.pop('redis_id')

        if not redis_name:
            if 'redis_name' in ent_kwargs:
                redis_name = ent_kwargs.pop('redis_name')

        cls_kwargs = {attr: ent_kwargs[attr] for attr in cls.get_entry_fields() if attr not in ['redis_id', 'redis_name']}
        return cls(redis_id=redis_id, redis_name=redis_name, **cls_kwargs)

    def as_dict(self, include_redis_attrs: bool = True) -> Mapping[str, Any]:
        ent_dict = asdict(self)

        if include_redis_attrs:
            return ent_dict

        return {attr: value for attr, value in ent_dict.items() if attr not in ['redis_id', 'redis_name']}

    @classmethod
    def fetch(cls, helper: RedisHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        if redis_name:
            if not helper.redis.hexists(redis_id, redis_name):
                raise KeyError(f'No hash entry found for Redis key "{redis_id}" for entry "{redis_name}"')

            ent_bytes = helper.redis.hget(redis_id, redis_name)
        else:
            if not helper.redis.exists(redis_id):
                raise KeyError(f'No entry found for Redis key "{redis_id}"')

            ent_bytes = helper.redis.get(redis_id)

        return RedisEntry.decode_entry(ent_bytes)

    def store(self, helper: RedisHelper) -> bool:
        ent_bytes = RedisEntry.encode_entry(self, as_mapping=self.store_as_mapping)

        if self.is_hashmap:
            return helper.redis.hset(self.redis_id, self.redis_name, ent_bytes) > 0
        else:
            return helper.redis.set(self.redis_id, ent_bytes)

    @classmethod
    def decode_entry(cls, entry_bytes: bytes) -> RedisEntry:
        try:
            ent = pickle.loads(entry_bytes)

            if isinstance(ent, RedisEntry):
                return ent

            ent['store_as_mapping'] = True
            import pdb
            pdb.set_trace()
            return cls.from_dict(**ent)

        except Exception as ex:
            raise Exception(f'Error decoding entry using pickle: {ex}')

    @classmethod
    def encode_entry(cls, entry: RedisEntry, as_mapping: bool = None) -> bytes:
        try:
            return pickle.dumps(entry.as_dict(include_redis_attrs=True) if as_mapping is True else entry)
        except Exception as ex:
            raise Exception(f'Error encoding entry for "{entry.redis_id}" (entry name: "{entry.redis_name or "N/A"}") using pickle: {ex}')


@dataclass
class Reminder(RedisEntry):
    member_id: str = field(default_factory=str)
    member_name: str = field(default_factory=str)
    channel_id: str = field(default_factory=str)
    channel_name: str = field(default_factory=str)
    provided_when: str = field(default_factory=str)
    trigger_ts: float = field(default_factory=float)
    created_ts: float = field(default_factory=float)
    content: str = field(default_factory=str)

    is_complete: bool = field(default=False)

    @property
    def trigger_dt(self):
        return datetime.fromtimestamp(self.trigger_ts)

    @property
    def created_dt(self):
        return datetime.fromtimestamp(self.created_ts)

    def __post_init__(self):
        dt_now = datetime.now()

        if self.trigger_dt < dt_now:
            self.is_complete = True


@dataclass
class DummyEnt(RedisEntry):
    value_one: str = field(default_factory=str)
    dt_value: datetime = field(default_factory=datetime)
    def_value: int = field(default=5)
