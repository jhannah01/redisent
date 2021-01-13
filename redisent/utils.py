from __future__ import annotations

import asyncio
import aioredis
import redis
import dateparser
import logging
import functools

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dataclasses import dataclass, field
from typing import Union, Optional, Any, Mapping, Dict

logger = logging.getLogger(__name__)

# Constants / Complex Types
RedisPoolType = Union[aioredis.ConnectionsPool, redis.ConnectionPool]
RedisPrimitiveType = Union[int, float, str, bytes]

REDIS_URL: str = 'redis://rpi04.synistree.com'
LOG_LEVEL: int = logging.INFO


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

    def __post_init__(self) -> None:
        res_time = dateparser.parse(self.provided_when, settings={'PREFER_DATES_FROM': 'future'})
        if not res_time:
            raise ValueError(f'Unable to resolve provided "when": {self.provided_when}')

        self.resolved_time = res_time

    @classmethod
    def build(cls, provided_when: str, created_ts: Union[float, int, datetime] = None) -> FuzzyTime:
        kwargs: Dict[str, Any] = {'provided_when': provided_when}

        if created_ts:
            kwargs['created_time'] = datetime.fromtimestamp(created_ts) if isinstance(created_ts, (int, float,)) else created_ts

        return FuzzyTime(**kwargs)

    @classmethod
    def from_dict(cls, dict_mapping: Mapping[str, Any]) -> FuzzyTime:
        return FuzzyTime(provided_when=dict_mapping['provided_when'], created_time=dict_mapping.get('created_time', None))


def force_async(fn):
    pool = ThreadPoolExecutor()
    try:
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            future = pool.submit(fn, *args, **kwargs)
            return asyncio.wrap_future(future)

        return _wrapper
    except Exception as ex:
        logger.exception(f'Error calling force_async wrapped function: {ex}')
    finally:
        if pool:
            pool.shutdown()


def force_sync(loop=None):
    loop = loop or asyncio.get_event_loop()

    if not loop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def _inner(fn):
        if not asyncio.iscoroutine(fn) and not asyncio.iscoroutinefunction(fn):
            raise TypeError(f'Cannot force decorated function "{fn.__name__}" to run async. It is not a coroutine.')

        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            res = fn(*args, **kwargs)

            if asyncio.iscoroutine(res):
                return loop.run_until_complete(res)

            return res

        return _wrapper

    return _inner