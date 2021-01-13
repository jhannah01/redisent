import pytest
import fakeredis.aioredis

from datetime import datetime, timedelta
from redisent.helpers import RedisentHelper

# Symlinked from ../examples/reminder.py
from reminder import Reminder


def build_reminder(use_dt: datetime = None, num_minutes: int = 5, created_ts: float = None) -> Reminder:
    use_dt = use_dt or datetime.now()
    trigger_dt = use_dt + timedelta(minutes=num_minutes)

    rem_kwargs = {'redis_id': 'reminders', 'member_id': 12345, 'member_name': 'pytest User',
                  'channel_id': 54321, 'channel_name': '#pytest',
                  'provided_when': f'in {num_minutes} minutes', 'content': 'pytest reminder content',
                  'trigger_ts': trigger_dt.timestamp()}

    if created_ts:
        rem_kwargs['created_ts'] = use_dt.timestamp()

    return Reminder(**rem_kwargs)


@pytest.mark.asyncio
async def test_async_store_reminder(fake_server):
    r_pool = await fakeredis.aioredis.create_redis_pool(fake_server)

    rem = build_reminder()

    try:
        rh = RedisentHelper(redis_pool=r_pool, use_async=True)

        async with rh.wrapped_redis(op_name=f'hexists("reminders", "{rem.redis_name}")') as r_conn:
            res = await r_conn.hexists('reminders', rem.redis_name)
            assert not res, f'Found unexpected, existing key for reminder "{rem.redis_name}" in Redis key "reminders"'

        res = await rem.store_async(rh)
        assert res > 0, f'Bad return value for store(): {res} (should be > 0)'

        rem_fetched = await Reminder.fetch_async(helper=rh, redis_id='reminders', redis_name=rem.redis_name)
        assert rem_fetched, f'No response back fetching "reminder" entry for "{rem.redis_name}". Got: {rem_fetched}'

        assert rem == rem_fetched, f'Fetched entry does not match original.\nFetched:\n{rem_fetched.dump()}\nCreated:\n{rem.dump()}'

        async with rh.wrapped_redis(op_name=f'hdel("reminders", "{rem.redis_name}")') as r_conn:
            res = await r_conn.hdel('reminders', rem.redis_name)
            assert res, f'Bad return from hdel of "{rem.redis_name}" in "reminders" Redis key. Got: {res}'
    finally:
        if r_pool:
            r_pool.close()
            await r_pool.wait_closed()


def test_blocking_store_reminder(redis):
    rh = RedisentHelper(redis_pool=redis, use_async=False)
    rem = build_reminder()

    with rh.wrapped_redis(op_name=f'hexists("reminders", "{rem.redis_name}")') as r_conn:
        res = r_conn.hexists('reminders', rem.redis_name)
        assert not res, f'Found unexpected, existing key for reminder "{rem.redis_name}" in Redis key "reminders"'

    res = rem.store(rh)
    assert res > 0, f'Bad return value for store(): {res} (should be > 0)'

    rem_fetched = Reminder.fetch(helper=rh, redis_id='reminders', redis_name=rem.redis_name)
    assert rem_fetched, f'No response back fetching "reminder" entry for "{rem.redis_name}". Got: {rem_fetched}'

    assert rem == rem_fetched, f'Fetched entry does not match original.\nFetched:\n{rem_fetched.dump()}\nCreated:\n{rem.dump()}'

    with rh.wrapped_redis(op_name=f'hdel("reminders", "{rem.redis_name}")') as r_conn:
        res = r_conn.hdel('reminders', rem.redis_name)
        assert res, f'Bad return from hdel of "{rem.redis_name}" in "reminders" Redis key. Got: {res}'