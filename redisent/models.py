from __future__ import annotations

from dataclasses import is_dataclass, dataclass, field, fields, asdict
from datetime import datetime
from typing import Mapping, Any, Union

from redisent.errors import RedisError
from redisent.redis import RedisHelper
from redisent.utils import RedisPoolConnType, FuzzyTime


@dataclass
class RedisEntry:
    redis_id: str = field(init=False)
    store_encoded: bool = field(default=True)

    def __new__(cls, *args, **kwargs):
        if not is_dataclass(cls):
            raise NotImplementedError(f'All instances of "{cls.__name__}" must be decorated with @dataclass')

        return super(RedisEntry, cls).__new__(cls)

    def as_dict(self) -> Mapping[str, Any]:
        return {attr: val for attr, val in asdict(self).items() if not attr.startswith('_') and attr not in ['redis_id', 'store_encoded']}


@dataclass()
class Reminder(RedisEntry):
    member_id: str = field(default_factory=str)
    trigger_ts: float = field(default_factory=float)

    created_ts: float = field(default_factory=float)
    member_name: str = field(default_factory=str)
    channel_id: str = field(default_factory=str)
    channel_name: str = field(default_factory=str)
    provided_when: str = field(default_factory=str)
    content: str = field(default_factory=str)

    is_complete: bool = field(default=False)

    def __post_init__(self) -> None:
        self.redis_id = 'reminders'
        self.store_encoded = True

    @property
    def reminder_key(self) -> str:
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

    @classmethod
    async def from_redis(cls, member_id: str = None, trigger_ts: float = None) -> Mapping[str, Reminder]:
        entry_key = f'{member_id or "*"}:{trigger_ts or "*"}' if trigger_ts or member_id else None

        if member_id and trigger_ts:
            if not await RedisHelper.hexists(self.redis_id, entry_key):
                raise Exception(f'Unable to find reminder ID "{entry_key}" in Redis')

            ent_dict = await RedisHelper.hget(self.redis_id, entry_key, decode_value=True)
            return {entry_key: Reminder.from_dict(ent_dict)}

        reminders = {}
        rem_entries = await RedisHelper.hget(self.redis_id) or {}

        for red_id, red_ent in rem_entries.items():
            red_id = red_id.decode('utf-8')

            mem, trg = red_id.split(':')
            if member_id and mem != member_id:
                continue

            if trigger_ts and trg != trigger_ts:
                continue

            reminders[red_id] = Reminder.from_dict(red_ent)

        return reminders

    async def to_redis(self, redis_pool: RedisPoolConnType = None):
        if await RedisHelper.hexists(self.redis_id, self.reminder_key, pool_or_conn=redis_pool):
            print(f'Overwriting reminder entry for "{self.reminder_key}" in Redis (already exists)')

        await RedisHelper.hset(self.redis_id, self.reminder_key, self.as_dict(), encode_value=True, pool_or_conn=redis_pool)

    @classmethod
    def build(cls, member_id: str, member_name: str, channel_id: str, channel_name: str, provided_when: Union[str, FuzzyTime], content: str, created_at: datetime = None):
        if not isinstance(provided_when, FuzzyTime):
            provided_when = FuzzyTime.build(provided_when, created_ts=int(created_at.timestamp()) if created_at else None)

        trigger_ts = provided_when.resolved_time.timestamp()

        return Reminder(member_id=member_id, trigger_ts=trigger_ts, member_name=member_name, channel_id=channel_id, channel_name=channel_name,
                        provided_when=provided_when.provided_when, content=content, created_ts=created_at.timestamp() if created_at else None)
