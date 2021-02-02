import pytest

from datetime import datetime
from pprint import pformat
from redisent import RedisentHelper, RedisError, RedisEntry


@pytest.mark.asyncio
async def test_async_redis(use_fake_aioredis):
    r_pool = await RedisentHelper.build_pool_async(redis_uri='redis://localhost')

    try:
        rh = RedisentHelper(redis_pool=r_pool)

        async with rh.wrapped_redis(op_name='set(blarg, 5.7)') as r_conn:
            res = await r_conn.set('blarg', 5.7)
        assert res, f'Bad return from set(): {res}'

        async with rh.wrapped_redis(op_name='exists(blarg)') as r_conn:
            res = await r_conn.exists('blarg')
        assert res, f'Key "blarg" did not return True for exists(). Got: {res}'

        async with rh.wrapped_redis(op_name='get(blarg)') as r_conn:
            res = float(await r_conn.get('blarg'))
        assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

        print(f'Received matching object back:\n{pformat(res, indent=4)}')

        async with rh.wrapped_redis(op_name='delete(blarg)') as r_conn:
            res = await r_conn.delete('blarg')
            assert res > 0, f'Bad return from delete(blarg): {res}'

        async with rh.wrapped_redis(op_name='hset("beep", "boop", ...)') as r_conn:
            res = await r_conn.hset('beep', 'boop', 40.66)
            assert res, f'Bad return from hset(beep, boop, ...): {res}'

        async with rh.wrapped_redis(op_name='hexists("beep", "boop")') as r_conn:
            res = await r_conn.hexists('beep', 'boop'),
            assert res, 'Cannot find hash key for "beep" -> "boop" just created'

        async with rh.wrapped_redis(op_name='hget("beep", "boop")') as r_conn:
            res = float(await r_conn.hget('beep', 'boop'))
            assert res == 40.66, f'Fetched value of "beep" -> "boop" does not match 40.66. Got: {res}'

        async with rh.wrapped_redis(op_name='hdel(beep, boop)') as r_conn:
            res = await r_conn.hdel('beep', 'boop')
            assert res > 0, f'Bad return from hdel(beep, boop): {res}'

    finally:
        if r_pool:
            r_pool.close()
            await r_pool.wait_closed()


def test_blocking_redis(use_fake_redis):
    pool = RedisentHelper.build_pool_sync(redis_uri='redis://localhost')
    rh = RedisentHelper(pool, is_async=False)

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
        assert res, f'Bad return from hset(beep, boop, ...): {res}'

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


def test_bad_sync_redis_value(use_fake_redis):
    r_pool = RedisentHelper.build_pool_sync(redis_uri='localhost')
    rh = RedisentHelper(r_pool, is_async=False)

    with pytest.raises(RedisError):
        with rh.wrapped_redis(op_name='set(bad_val, ...)') as r_conn:
            r_conn.set('bad_val', {'one': 1, 'oh_no': datetime.now()})


@pytest.mark.asyncio
async def test_bad_async_redis_value(use_fake_aioredis):
    r_pool = await RedisentHelper.build_pool_async(redis_uri='redis://localhost')
    rh = RedisentHelper(r_pool)

    try:
        async with rh.wrapped_redis(op_name='set(bad_val, ...)') as r_conn:
            await r_conn.set('bad_val', {'one': 1, 'oh_no': datetime.now()})
    except RedisError as ex:
        print(f'Caught expected RedisError: "{ex}"')
        print(f'repr(ex):\n{repr(ex)}')
        print(f'Dumping Exception:\n{ex.dump()}')
    except Exception as ex:
        pytest.fail(f'Received un-expected exception instead of "RedisError": {ex}', True)


def test_redis_error():
    try:
        raise RedisError('Oh what a dumb error', related_command=None, extra_attrs={'value': 5, 'ts': datetime.now()})
    except RedisError as ex:
        print(f'Caught expected dummy RedisError: "{ex}"')
        print(f'repr(ex):\n{repr(ex)}')
        print(f'Dumping Exception:\n{ex.dump()}')
    except Exception as ex:
        pytest.fail(f'Received un-expected exception instead of "RedisError": {ex}', True)
