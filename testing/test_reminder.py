import unittest

from datetime import datetime, timedelta
from redisent.helpers import RedisentHelper

# Symlinked from ../examples/reminder.py
from reminder import Reminder

from conftest import tmp_redis


class TestReminder(unittest.TestCase):
    def setUp(self):
        pool = RedisentHelper.build_pool('localhost')
        self.rh = RedisentHelper(pool, use_redis=tmp_redis)

    def _build_reminder(self, use_dt: datetime = None, num_minutes: int = 5, created_ts: float = None,
                        create_invalid: bool = False):
        use_dt = use_dt or datetime.now()
        trigger_dt = use_dt + timedelta(minutes=num_minutes)

        rem_kwargs = {'member_id': 12345, 'member_name': 'pytest User',
                      'channel_id': 54321, 'channel_name': '#pytest',
                      'provided_when': f'in {num_minutes} minutes', 'content': 'pytest reminder content',
                      'trigger_ts': trigger_dt.timestamp()}

        if created_ts:
            rem_kwargs['created_ts'] = use_dt.timestamp()

        if create_invalid:
            rem_kwargs['redis_name'] = {'bad': 'values', 'other': 5}

        return Reminder(**rem_kwargs)  # type: ignore[arg-type]

    def test_store_reminder(self):
        rem = self._build_reminder()

        with self.rh.wrapped_redis(op_name=f'hexists("reminders", "{rem.redis_name}")') as r_conn:
            res = r_conn.hexists('reminders', rem.redis_name)
            assert not res, f'Found unexpected, existing key for reminder "{rem.redis_name}" in Redis key "reminders"'

        res = rem.store(self.rh)
        assert res, f'Bad return value for store(): {res} (should be True (1) if not set, False (0) means overwritten)'

        rem_fetched = Reminder.fetch(helper=self.rh, redis_id='reminders', redis_name=rem.redis_name)
        assert rem_fetched, f'No response back fetching "reminder" entry for "{rem.redis_name}". Got: {rem_fetched}'

        assert rem == rem_fetched, f'Fetched entry does not match original.\nFetched:\n{rem_fetched.dump()}\nCreated:\n{rem.dump()}'

        print(f'Successfully retrieved Reminder entry back. Dump:\n{rem.dump()}')

        with self.rh.wrapped_redis(op_name=f'hdel("reminders", "{rem.redis_name}")') as r_conn:
            res = r_conn.hdel('reminders', rem.redis_name)
            assert res, f'Bad return from hdel of "{rem.redis_name}" in "reminders" Redis key. Got: {res}'

        assert rem.is_hashmap, f'Assert that reminder is a hash map failed. Got: {rem}'

    # def test_store_bad_reminder():
    #     rem = build_reminder(create_invalid=True)
    #     rem.store(self.rh)
    #     print(rem.dump())
