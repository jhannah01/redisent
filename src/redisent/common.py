import redis
import redislite

from typing import Union


RedisPrimitiveType = Union[str, int, float, bytes, bytearray]
RedisType = Union[redis.Redis, redis.StrictRedis, redislite.Redis, redislite.StrictRedis]


def is_redislite_instance(redis_instance: RedisType) -> bool:
    return isinstance(redis_instance, (redislite.Redis, redislite.StrictRedis,))


__all__ = ['RedisPrimitiveType', 'RedisType', 'is_redislite_instance']
