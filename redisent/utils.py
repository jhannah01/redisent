
import aioredis

from contextlib import asynccontextmanager
from typing import Union

from redisent.errors import RedisError

RedisPoolConnType = Union[aioredis.Redis, aioredis.ConnectionsPool, aioredis.RedisConnection]

# Constants
REDIS_URL: str = 'redis://rpi04.synistree.com'


@asynccontextmanager
async def wrapped_redis(pool_or_conn: RedisPoolConnType = None, operation_name: str = None):
    try:
        pool_or_conn = pool_or_conn or await aioredis.create_redis_pool(REDIS_URL)
    except Exception as ex:
        raise RedisError(f'Error attempting to connect to "{REDIS_URL}": {ex}', base_exception=ex, related_command=operation_name)

    try:
        yield pool_or_conn
    except Exception as ex:
        raise RedisError(f'Redis Error executing "{operation_name or "Unknown"}": {ex}', base_exception=ex, related_command=operation_name)
    finally:
        pool_or_conn.close()
        await pool_or_conn.wait_closed()
