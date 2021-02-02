from __future__ import annotations

import asyncio
import aioredis
import redis
import logging

from contextlib import contextmanager, asynccontextmanager
from typing import Union

from redisent import RedisError

logger = logging.getLogger(__name__)

# Constants / Complex Types
RedisPoolType = Union[aioredis.ConnectionsPool, redis.ConnectionPool]
RedisPrimitiveType = Union[int, float, str, bytes]

LOG_LEVEL: int = logging.INFO
