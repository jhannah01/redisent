import redis
from typing import Union

RedisPrimitiveType = Union[str, int, float, bytes, bytearray]

# Dummy type so consumers do not need to import from "redis"
RedisType = redis.Redis
RedisPoolType = redis.ConnectionPool

__all__ = ['RedisPrimitiveType', 'RedisType', 'RedisPoolType']
