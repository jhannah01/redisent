from datetime import datetime

import aiounittest

import fakeredis.aioredis

from redisent import RedisHelper

redis_server = fakeredis.FakeServer()


class RedisHelperTestCase(aiounittest.AsyncTestCase):
    async def build_redis_pool(self, encoding=None):
        return await fakeredis.aioredis.create_redis_pool(redis_server, encoding=encoding)

    async def test_redishelper__set_exists_get_keys(self):
        redis_pool = await self.build_redis_pool(encoding='utf-8')

        try:
            with await redis_pool as redis_conn:
                await RedisHelper.set('blarg', float(2.5), encode_value=False, redis_pool=redis_conn)

                assert await RedisHelper.exists('blarg', redis_pool=redis_conn), 'Newly set key "blarg" does not appear to exist'

                ent = await RedisHelper.get('blarg', decode_value=False, decode_response=True, redis_pool=redis_conn)

                assert ent, 'Received no entry back for "blarg" (only None)'
                assert ent == '2.5', f'Received different value back: {ent}'

                print(f'Received matching response back from Redis for "blarg": {ent}')

                redis_keys = await RedisHelper.keys('*', redis_pool=redis_pool)
                print(f'All keys: {redis_keys}')
        finally:
            redis_pool.close()
            await redis_pool.wait_closed()

    async def test_redishelper__hget_hexists_hset_hkeys(self):
        redis_pool = await self.build_redis_pool(encoding=None)
        dt_now = datetime.now()

        try:
            with await redis_pool as redis_conn:
                await RedisHelper.hset('hblarg', 'entry', {'val': 5, 'dt': dt_now}, encode_value=True, redis_pool=redis_conn)

                assert await RedisHelper.hexists('hblarg', 'entry', redis_pool=redis_conn), f'Newly set hkey "hblarg" does not appear to exist'

                ent = await RedisHelper.hget('hblarg', 'entry', decode_value=True, redis_pool=redis_conn)

                print(f'Fetched entry: {ent}')
                assert ent['val'] == 5, f'Expected value of "hblarg[val]" to be "5", got: {ent}'
                assert ent['dt'] == dt_now, f'Expected value of "hblarg[dt]" should be "{dt_now}", got: {ent}'

                print(f'Received match hash entry back from Redis for "hblarg": {ent}')
        finally:
            redis_pool.close()
            await redis_pool.wait_closed()