import pytest

import fakeredis.aioredis

from pprint import pformat
from redisent import RedisentHelper


@pytest.mark.asyncio
async def test_async_redis(fake_server):
    r_pool = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisentHelper.build(r_pool, use_async=True)

        await rh.set('blarg', 5.7)
        assert await rh.exists('blarg'), 'Set key "blarg" but not found after setting.'

        res = float(await rh.get('blarg', encoding=None))
        assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

        print(f'Received matching object back\n{pformat(res, indent=4)}')

        await rh.hset('beep', 'boop', 40.66)
        assert await rh.hexists('beep', 'boop'), 'Cannot find hash key for "beep" -> "boop"'

        res = await rh.hget('beep', 'boop', use_return_type=float)
        assert res == 40.66, f'Fetched value of "beep" -> "boop" does not match 40.66. Got: {res}'
    finally:
        r_pool.close()
        await r_pool.wait_closed()


def test_blocking_redis(redis):
    rh = RedisentHelper.build(redis, use_async=False)

    rh.set('blarg', 5.7)
    assert rh.exists('blarg'), 'Set key "blarg" but not found after setting.'

    res = float(rh.get('blarg'))
    assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

    print(f'Received matching object back\n{pformat(res, indent=4)}')


def test_blocking_hash_redis(redis):
    rh = RedisentHelper.build(redis, use_async=False)

    assert rh.hset('beep', 'boop', 40.66), 'Unable to set hash value in "beep" -> "boop"'
    assert rh.hexists('beep', 'boop'), 'Cannot find hash key for "beep" -> "boop"'

    res = rh.hget('beep', 'boop', use_return_type=float)
    assert res == 40.66, f'Fetched value of "beep" -> "boop" does not match 40.66. Got: {res}'

    all_ents = rh.hgetall('beep')

    assert 'boop' in all_ents, f'Missing "boop" entry in hgetall keys: "{all_ents.keys()}"'

    print(f'Received full hash entry for "beep":\n{pformat(all_ents, indent=4)}')

'''
@pytest.mark.asyncio
async def test_async_helper__set_exists_keys_get_delete(fake_server):
    r_pool = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisentHelper.build(r_pool, use_async=False)

        await rh.set('blarg', 10)

        redis_keys = await rh.keys('*')

        assert 'blarg' in redis_keys, f'Cannot find expected key "blarg". All keys: {redis_keys}'

        res = await rh.get('blarg')

        assert res == 10, f'Fetched value just set is not 10. Value: {res}'

        print(f'Successfully fetched "blarg" back with value "{res}"')

        assert await rh.delete('blarg'), f'Failed to delete new key "blarg"'
    finally:
        r_pool.close()
        await r_pool.wait_closed()
'''
'''
@pytest.mark.asyncio
async def test_redishelper__hset_hexists_hkeys_hget_hgetall_hdelete(fake_server):
    r_conn = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisHelper(r_conn)

        await rh.hset('blarg', 'value', {'test': 10}, encode_value=True, pool=r_conn)

        assert await rh.hexists('blarg', 'value', pool=r_conn), f'Newly set hash "blarg" -> "value" fails hexists'

        redis_keys = await rh.hkeys('blarg', pool=r_conn)

        assert 'value' in redis_keys, f'Cannot find expected key "blarg". All keys: {redis_keys}'

        res = await rh.hget('blarg', 'value', pool=r_conn)

        assert res['test'] == 10, f'Fetched value of "blarg" -> "value" was not 10. Value: {res}'

        print(f'Successfully fetched "blarg" back with value "{res}"')

        all_ents = await rh.hgetall('blarg', pool=r_conn)

        assert 'value' in all_ents, f'Cannot find entry in hash key "blarg" for "value" using hgetall. Keys: "{all_ents}"'

        assert all_ents['value'] == {'test': 10}, f'Invalid value found for entry "value" in hash key "blarg". Should be 10, All fetched entries: "{all_ents["value"]}"'

        assert await rh.hdelete('blarg', 'value', pool=r_conn), f'Failed to delete new entry "value" in key "blarg"'
    finally:
        if r_conn:
            r_conn.close()
            await r_conn.wait_closed()


@pytest.mark.asyncio
async def test_redishelper__get_bad_entry(fake_server):
    r_conn = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisHelper(r_conn)

        with pytest.raises(RedisError) as exc_info:
            res = await rh.get('fake_blah', missing_okay=False, pool=r_conn)

        assert 'Does not exist' in str(exc_info), f'Expected to receive exception when fetching bad redis key using missing_okay=False. Got instead: {exc_info}'

        assert await rh.get('fake_blah', missing_okay=True, pool=r_conn) is None, 'Excepted exception to be raised fetching bad Redis entry using missing_okay=True'
    finally:
        if r_conn:
            r_conn.close()
            await r_conn.wait_closed()

'''