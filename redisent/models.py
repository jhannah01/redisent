from __future__ import annotations

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from typing import Mapping, Any, Union

from redisent.errors import RedisError
from redisent.redis import RedisHelper
from redisent.utils import RedisPoolConnType


class RedisEntry:
    redis_id: str
    store_encoded: bool

    entry_fields: Mapping[str, Any] = {}

    def __new__(cls, *args, **kwargs):
        assert is_dataclass(cls), f'All instances of "{cls.__name__}" must be decorated with @dataclass'

    def __init__(self, redis_id: str, store_encoded: bool = True, entry_fields: Mapping[str, Any] = None):
        self.redis_id = redis_id
        self.store_encoded = store_encoded

        if entry_fields:
            self.entry_fields = entry_fields

    @classmethod
    async def load(cls, redis_id: str, is_encoded: bool = True, redis_pool: RedisPoolConnType = None):
        if not await RedisHelper.exists(redis_id, pool_or_conn=redis_pool):
            raise RedisError(f'No entry with ID "{redis_id}" found in Redis', related_command=f'exists("{redis_id}")')

        ent_redis = await RedisHelper.get(redis_id, decode_value=is_encoded, pool_or_conn=redis_pool)

        if not is_encoded:
            return ent_redis

        return cls(redis_id, store_encoded=is_encoded, entry_fields=ent_redis)

    async def save(self, redis_pool: RedisPoolConnType = None):
        if await RedisHelper.exists(self.redis_id, pool_or_conn=redis_pool):
            print(f'Overwriting entry for "{self.redis_id}" in Redis (already exists)')

        await RedisHelper.set(self.redis_id, self.entry_fields, encode_value=self.store_encoded, pool_or_conn=redis_pool)


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
