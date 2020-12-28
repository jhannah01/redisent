import pytest

import fakeredis.aioredis

from datetime import datetime
from pprint import pformat
from redisent import RedisHelper, RedisError


@pytest.mark.asyncio
async def test_wrapped_redis(fake_server):
    r_conn = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisHelper(r_conn)
        dt_now = datetime.now()

        await rh.set('blarg', {'val': 5, 'dt': dt_now}, pool=r_conn)
        assert await rh.exists('blarg', pool=r_conn), 'Newley set "blarg" key not found after setting.'

        res = await rh.get('blarg', decode_value=True, pool=r_conn)
        assert res['dt'] == dt_now, f'Mismatch on committed now datetime. dt_now: {dt_now}, got: {res["dt"]}'

        print(f'Received matching object back\n{pformat(res, indent=4)}')
    finally:
        if r_conn:
            r_conn.close()
            await r_conn.wait_closed()


@pytest.mark.asyncio
async def test_redishelper__set_exists_keys_get_delete(fake_server):
    r_conn = await fakeredis.aioredis.create_redis_pool(fake_server)

    try:
        rh = RedisHelper(r_conn)
    
        await rh.set('blarg', 10, pool=r_conn)

        redis_keys = await rh.keys('*', pool=r_conn)

        assert 'blarg' in redis_keys, f'Cannot find expected key "blarg". All keys: {redis_keys}'

        res = await rh.get('blarg', pool=r_conn)

        assert res == 10, f'Fetched value just set is not 10. Value: {res}'

        print(f'Successfully fetched "blarg" back with value "{res}"')

        assert await rh.delete('blarg', pool=r_conn), f'Failed to delete new key "blarg"'
    finally:
        if r_conn:
            r_conn.close()
            await r_conn.wait_closed()


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

