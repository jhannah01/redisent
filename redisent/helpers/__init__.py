from __future__ import annotations

import aioredis
import redis

from typing import Any, Mapping, Optional, Union

RedisPoolType = Union[aioredis.ConnectionsPool, redis.ConnectionPool]
RedisPrimitiveType = Union[int, float, str, bytes]

REDIS_URL = 'redis://rpi04.synistree.com'


class RedisError(Exception):
    message: str

    base_exception: Optional[Exception]
    related_command: str
    extra_attrs: Mapping[str, Any] = {}

    def __init__(self, message: str, base_exception: Exception = None, related_command: str = None, extra_attrs: Mapping[str, Any] = None) -> None:
        super(RedisError, self).__init__(message)

        self.message = message
        self.base_exception = base_exception
        self.related_command = related_command or 'Unknown'
        self.extra_attrs = extra_attrs or {}

    def __repr__(self) -> str:
        repr_out = f'<RedisError(message="{self.message}"'

        if self.base_exception:
            repr_out = f'{repr_out}, base_exception="{self.base_exception}"'

        if self.extra_attrs:
            ext_attrs = '", "'.join([f'{attr} -> {val}' for attr, val in self.extra_attrs.items()])
            repr_out = f'{repr_out}, extra_attrs="{ext_attrs}"'

        return f'{repr_out})>'

    def __str__(self) -> str:
        str_out = f'Redis Error '

        if self.related_command:
            str_out = f'{str_out} with command "{self.related_command}"'

        return f'{str_out}: {self.message}'


class RedisentHelper:
    redis_pool: RedisPoolType

    def __init__(self, redis_pool: RedisPoolType) -> None:
        self.redis_pool = redis_pool

    @classmethod
    def build(cls, redis_url: str = REDIS_URL):
        raise NotImplementedError('Subclasses of RedisentHelper must implement the class build method')