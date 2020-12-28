from __future__ import annotations

import aioredis
import dateparser
import logging

from datetime import datetime
from dataclasses import dataclass, field
from typing import Union, Optional, Any, Mapping


logger = logging.getLogger(__name__)

# Constants / Complex Types
RedisPoolConnType = Union[aioredis.Redis, aioredis.ConnectionsPool, aioredis.RedisConnection]

REDIS_URL: str = 'redis://rpi04.synistree.com'
LOG_LEVEL: int = logging.INFO


@dataclass
class FuzzyTime:
    provided_when: str = field()

    created_time: datetime = datetime.now()
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

    def __post_init__(self) -> None:
        res_time = dateparser.parse(self.provided_when, settings={'PREFER_DATES_FROM': 'future'})
        if not res_time:
            raise ValueError(f'Unable to resolve provided "when": {self.provided_when}')

        self.resolved_time = res_time

    @classmethod
    def build(cls, provided_when: str, created_ts: Union[float, int, datetime] = None) -> FuzzyTime:
        kwargs = {'provided_when': provided_when}

        if created_ts:
            if isinstance(created_ts, (int, float,)):
                created_ts = datetime.fromtimestamp(created_ts)

            kwargs['created_time'] = created_ts

        return FuzzyTime(**kwargs)

    @classmethod
    def from_dict(cls, dict_mapping: Mapping[str, Any]) -> FuzzyTime:
        return FuzzyTime(provided_when=dict_mapping['provided_when'], created_time=dict_mapping.get('created_time', None))
