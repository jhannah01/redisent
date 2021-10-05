from __future__ import annotations

import dateparser

from datetime import datetime
from dataclasses import dataclass, field

from typing import Union, Optional, Mapping, Any, MutableMapping, ClassVar

from redisent import RedisEntry


@dataclass
class FuzzyTime:
    provided_when: str = field()

    created_time: datetime = field()
    resolved_time: datetime = field(init=False)

    @property
    def created_timestamp(self) -> float:
        return self.created_time.timestamp()

    @property
    def resolved_timestamp(self) -> float:
        return self.resolved_time.timestamp()

    @property
    def num_seconds_left(self) -> Optional[int]:
        dt_now = datetime.now()

        if dt_now > self.resolved_time:
            return None

        t_delta = (self.resolved_time - dt_now)

        return int(t_delta.total_seconds()) if t_delta else None

    def __post_init__(self, *args, **kwargs) -> None:
        res_time = dateparser.parse(self.provided_when, settings={'PREFER_DATES_FROM': 'future'})
        if not res_time:
            raise ValueError(f'Unable to resolve provided "when": {self.provided_when}')

        self.resolved_time = res_time

    @classmethod
    def build(cls, provided_when: str, created_ts: Union[float, int, datetime] = None) -> FuzzyTime:
        kwargs: MutableMapping[str, Any] = {'provided_when': provided_when}

        if created_ts:
            kwargs['created_time'] = datetime.fromtimestamp(created_ts) if isinstance(created_ts, (int, float,)) else created_ts

        return FuzzyTime(**kwargs)

    @classmethod
    def from_dict(cls, dict_mapping: Mapping[str, Any]) -> FuzzyTime:
        return FuzzyTime(provided_when=dict_mapping['provided_when'], created_time=dict_mapping.get('created_time', None))


@dataclass
class Reminder(RedisEntry):
    redis_id: ClassVar[str] = 'reminders'

    member_id: str = field(default_factory=str)
    member_name: str = field(default_factory=str)

    channel_id: str = field(default_factory=str)
    channel_name: str = field(default_factory=str)

    provided_when: str = field(default_factory=str)
    content: str = field(default_factory=str)

    trigger_ts: float = field(default_factory=float)
    created_ts: float = field(default_factory=float)

    is_complete: bool = field(default=False, compare=False)

    @property
    def trigger_dt(self) -> datetime:
        """
        Property wrapper for converting the float "trigger_ts" into "datetime"
        """

        return datetime.fromtimestamp(self.trigger_ts)

    @property
    def created_dt(self) -> datetime:
        """
        Property wrapper for converting the float "created_ts" into "datetime"
        """

        return datetime.fromtimestamp(self.created_ts)

    def __post_init__(self, *args, **kwargs) -> None:
        """
        Set the default "is_complete" field based on current timestamp and
        sets "redis_name" based on member and trigger attributes
        """

        self.redis_name = f'{self.member_id}:{self.trigger_ts}'

        if self.trigger_dt < datetime.now():
            self.is_complete = True
