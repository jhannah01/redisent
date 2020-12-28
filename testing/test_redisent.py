import pytest

from datetime import datetime
from pprint import pformat
from redisent import RedisHelper, wrapped_redis


@pytest.mark.asyncio
async def test_wrapped_redis(redis):
    dt_now = datetime.now()

    async with wrapped_redis(redis, 'set("blarg")') as conn:
        await RedisHelper.set('blarg', {'val': 5, 'dt': dt_now}, redis_pool=conn)

        assert await RedisHelper.exists('blarg', redis_pool=conn), 'Newley set "blarg" key not found after setting.'

        res = await RedisHelper.get('blarg', redis_pool=conn)
        assert res['dt'] == dt_now, f'Mismatch on committed now datetime. dt_now: {dt_now}, got: {res["dt"]}'

        print(f'Received matching object back\n{pformat(res, indent=4)}')


@pytest.mark.asyncio
async def test_redishelper_keys(redis):
    with await redis as redis_conn:
        await redis_conn.set('blarg', 'my_value')

    #async with wrapped_redis(redis, 'set("blarg")') as conn:
    #    await RedisHelper.set('blarg', {'val': 5, 'other': 2.5}, redis_pool=conn)

    redis_keys = await RedisHelper.keys('*', redis_pool=redis)

    assert 'blarg' in redis_keys, f'Cannot find expected key "blarg". All keys: {redis_keys}'


@pytest.mark.asyncio
async def test_redishelper_hkeys(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_exists(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_set(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_get(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_delete(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_hdelete(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_hexists(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_hset(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_hgetall(redis):
    pass


@pytest.mark.asyncio
async def test_redishelper_hget(redis):
    pass

