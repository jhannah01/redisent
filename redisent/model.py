import aioredis
import asyncio
import os
import logging

from base64 import urlsafe_b64encode
from typing import Optional, List, Mapping, Any

REDIS_URL = 'redis://rpi04.synistree.com'
logger = logging.getLogger(__name__)


class RedisObject:
    """
    Shared base class representing the fundamentally identifying attributes of a ``redisent`` entry
    """

    redis_id: str

    loop: asyncio.AbstractEventLoop
    redis_pool: aioredis.Redis

    def __init__(self, redis_id: str = None, loop: asyncio.AbstractEventLoop = None):
        self.redis_id = redis_id or urlsafe_b64encode(os.urandom(12)).decode('utf-8')

        self.loop = loop or asyncio.get_event_loop()
        self.redis_pool = self.loop.run_until_complete(aioredis.create_redis_pool(REDIS_URL, encoding='utf-8'))

    def __del__(self):
        if self.redis_pool and not self.redis_pool.closed:
            try:
                self.redis_pool.close()
                self.loop.run_until_complete(self.redis_pool.wait_closed())
            except Exception as ex:
                logger.error(f'Error while attempting to close the Redis pool in __del__: {ex}')
                return

    async def exists(self, key: str = None) -> Optional[bool]:
        key = key or self.redis_id

        try:
            with await self.redis_pool as redis:
                return (await redis.exists(key)) == 1
        except Exception as ex:
            logger.error(f'Error checking if "{key}" exists in Redis: {ex}')
            return None

    async def delete(self, key: str = None) -> Optional[bool]:
        key = key or self.redis_id

        try:
            with await self.redis_pool as redis:
                if await redis.exists(key) != 1:
                    logger.error(f'Unable to delete key "{key}" from Redis: No such key found')
                    return None

                return (await redis.delete(key)) > 0
        except Exception as ex:
            logger.error(f'Redis error encountered when attempting to delete key "{key}": {ex}')
            return None

    async def list_keys(self, pattern: str = None) -> Optional[List[str]]:
        pattern = pattern or '*'

        try:
            with await self.redis_pool as redis:
                return await redis.keys(pattern)
        except Exception as ex:
            logger.error(f'Error while attempting to list Redis keys (pattern: "{pattern}"): {ex}')
            return None

    def __bool__(self) -> bool:
        return self.loop.run_until_complete(self.exists())


class RedisDict(RedisObject):
    def __init__(self, redis_id: str = None, fields: Mapping[str, Any] = None, defaults: Mapping[str, Any] = None):
        super(RedisDict, self).__init__(redis_id)

        self.fields = fields or {}

        if defaults:
            for k, v in defaults.items():
                self[k] = v

    def __getitem__(self, key: str):
        if key in ['id', 'reids_id']:
            return self.redis_id

        if not self:
            raise KeyError(f'Object "{self.redis_id}" not found in Redis')

        if key not in self.fields:
            raise KeyError('Key "{key}" not found in "{self}" (ID: "{self.redis_id}")')

        self.loop.run_until_complete(self.fetch(key))

    async def fetch(self, key: str = None) -> Mapping[str, Any]:
        key = key or self.redis_id

        with await self.redis_pool as redis:
            if not await redis.exists(self.redis_id, key):
                raise KeyError(f'Cannot find key "{key}" in Redis under "{self.redis_id}"')

            try:
                return await redis.hget(self.redis_id, key)
            except Exception as ex:
                raise KeyError(f'Error fetching key "{key}" from dictionary "{self.redis_id}": {ex}')

