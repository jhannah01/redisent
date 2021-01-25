from __future__ import annotations

import aioredis
import redis
import logging

from typing import Union

logger = logging.getLogger(__name__)

# Constants / Complex Types
RedisPoolType = Union[aioredis.ConnectionsPool, redis.ConnectionPool]
RedisPrimitiveType = Union[int, float, str, bytes]

LOG_LEVEL: int = logging.INFO
