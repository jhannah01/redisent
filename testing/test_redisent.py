import pytest

import fakeredis.aioredis

from pprint import pformat
from redisent import RedisentHelper


@pytest.mark.asyncio
async def test_async_redis(fake_server):
    r_pool = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisentHelper(redis_pool=r_pool, use_async=True)

        async with rh.wrapped_redis_async(op_name='set(blarg=5.7)') as r_conn:
            res = await r_conn.set('blarg', 5.7)
        assert res, f'Bad return from set(): {res}'

        async with rh.wrapped_redis_async(op_name='exists(blarg)') as r_conn:
            res = await r_conn.exists('blarg')
        assert res, f'Key "blarg" did not return True for exists(). Got: {res}'

        async with rh.wrapped_redis_async(op_name='get(blarg)') as r_conn:
            res = float(await r_conn.get('blarg'))
        assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

        print(f'Received matching object back:\n{pformat(res, indent=4)}')

        op_name = 'hset("beep", "boop", ...) + hexists("beep", "boop") + hget("beep", "boop")'
        async with rh.wrapped_redis_async(op_name=op_name) as r_conn:
            res = await r_conn.hset('beep', 'boop', 40.66)
            assert res, f'Bad retrun from hset(): {res}'

            res = await r_conn.hexists('beep', 'boop'),
            assert res, 'Cannot find hash key for "beep" -> "boop" just created'

            res = float(await r_conn.hget('beep', 'boop'))
            assert res == 40.66, f'Fetched value of "beep" -> "boop" does not match 40.66. Got: {res}'
    finally:
        if r_pool:
            r_pool.close()
            await r_pool.wait_closed()


def test_blocking_redis(redis):
    rh = RedisentHelper(redis_pool=redis, use_async=False)

    with rh.wrapped_redis(op_name='set(blarg=5.7)') as r_conn:
        res = r_conn.set('blarg', 5.7)
    assert res, f'Bad return from set(): {res}'

    with rh.wrapped_redis(op_name='exists(blarg)') as r_conn:
        res = r_conn.exists('blarg')
    assert res, 'Set key "blarg" but not found after setting.'

    with rh.wrapped_redis(op_name='keys(*)') as r_conn:
        res = [val.decode('utf-8') for val in r_conn.keys('*')]
    assert res, 'No keys returned.'
    assert 'blarg' in res, f'Could not find set key "blarg" in keys. Got: {res}'

    with rh.wrapped_redis(op_name='get(blarg)') as r_conn:
        res = float(r_conn.get('blarg'))
    assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

    print(f'Received matching object back\n{pformat(res, indent=4)}')

    with rh.wrapped_redis(op_name='delete(blarg)') as r_conn:
        res = r_conn.delete('blarg')
    assert res > 0, f'Bad return from delete(): {res}'

    print('All regular tests complete')

    with rh.wrapped_redis(op_name='hset(beep, boop, ...)') as r_conn:
        res = r_conn.hset('beep', 'boop', 40.66)
    assert res == 0, f'Bad return from hset(): {res}'

    with rh.wrapped_redis(op_name='hexists(beep, boop)') as r_conn:
        res = r_conn.hexists('beep', 'boop')
    assert res, 'Set hash entry "boop" in key "beep" not found after setting.'

    with rh.wrapped_redis(op_name='hkeys(beep)') as r_conn:
        res = [val.decode('utf-8') for val in r_conn.hkeys('beep')]
    assert res, 'No hkeys returned for "beep".'
    assert 'boop' in res, f'Could not find hash entry "boop" in key "beep". Got: {res}'

    with rh.wrapped_redis(op_name='hget(beep, boop)') as r_conn:
        res = float(r_conn.hget('beep', 'boop'))
    assert res == 40.66, f'Fetched value of "boop" from key "beep" does not match set value (40.66). Got: {res}'

    print(f'Received matching hash entry "boop" from key "beep" back\n{pformat(res, indent=4)}')

    @rh.decode_entries(first_handler=lambda res: {k.decode('utf-8'): float(v) for k, v in res.items()})
    def get_all():
        with rh.wrapped_redis(op_name='hgetall(beep)') as r_conn:
            return r_conn.hgetall('beep')

    all_ents = get_all()
    assert 'boop' in all_ents, f'Missing "boop" entry in hgetall keys: "{all_ents.keys()}"'
    assert all_ents == {'boop': 40.66}, f'Expected single dictionary entry in hgetall result. Got: {all_ents}'

    print(f'Received full hash entry for "beep":\n{pformat(all_ents, indent=4)}')

    with rh.wrapped_redis(op_name='hdel(beep, boop)') as r_conn:
        res = r_conn.hdel('beep', 'boop')
    assert res > 0, f'Bad return from hdel(beep, boop): {res}'

    print('All hash tests complete')
