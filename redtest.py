from __future__ import annotations

import pickle
import logging

from datetime import datetime
from redis import StrictRedis
from typing import Mapping, Any, List, Optional
from dataclasses import dataclass, field, fields, asdict, is_dataclass

from redisent import RedisError

logger = logging.getLogger(__name__)


class RedisHelper:
    redis: StrictRedis

    def __init__(self, redis_conn: StrictRedis = None) -> None:
        self.redis = redis_conn or StrictRedis('rpi04.synistree.com', decode_responses=False)


@dataclass()
class RedisEntry:
    helper: RedisHelper = field(repr=False, metadata={'redis_field': True, 'internal': True})
    redis_id: str = field(metadata={'redis_field': True})
    store_as_mapping: bool = field(default=False, repr=False, metadata={'redis_field': True})

    redis_name: Optional[str] = field(default=None, metadata={'redis_field': True})

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
    def from_dict(cls, helper: RedisHelper, redis_id: str, redis_name: str = None, **ent_kwargs) -> RedisEntry:
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
    def fetch(cls, helper: RedisHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
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

    def store(self, helper: RedisHelper) -> bool:
        ent_bytes = RedisEntry.encode_entry(self, as_mapping=self.store_as_mapping)

        if self.is_hashmap:
            return helper.redis.hset(self.redis_id, self.redis_name, ent_bytes) > 0

        res = helper.redis.set(self.redis_id, ent_bytes)

        return True if res and res > 0 else False

    @classmethod
    def decode_entry(cls, helper: RedisHelper, entry_bytes: bytes) -> RedisEntry:
        try:
            ent = pickle.loads(entry_bytes)

            if isinstance(ent, RedisEntry):
                return ent

            ent['store_as_mapping'] = True
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

    dt_value: datetime = field(default=datetime.now())
    def_value: int = field(default=5)


hlp = RedisHelper()
b = hlp.redis.get('dummy_ent')
