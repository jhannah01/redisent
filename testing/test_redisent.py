# import pickle
import unittest

# from datetime import datetime
from pprint import pformat
from redisent import RedisentHelper  # , RedisError

from conftest import tmp_redis


class TestRedisent(unittest.TestCase):
    def setUp(self):
        pool = RedisentHelper.build_pool('localhost')
        self.rh = RedisentHelper(pool, use_redis=tmp_redis)

    def tearDown(self):
        super().tearDown()

    def test_redishelper(self):
        with self.rh.wrapped_redis(op_name='set(blarg=5.7)') as r_conn:
            res = r_conn.set('blarg', 5.7)
        assert res, f'Bad return from set(): {res}'

        with self.rh.wrapped_redis(op_name='exists(blarg)') as r_conn:
            res = r_conn.exists('blarg')
        assert res, 'Set key "blarg" but not found after setting.'

        with self.rh.wrapped_redis(op_name='keys(*)') as r_conn:
            res = [val.decode('utf-8') for val in r_conn.keys('*')]
        assert res, 'No keys returned.'
        assert 'blarg' in res, f'Could not find set key "blarg" in keys. Got: {res}'

        rh_keys = self.rh.keys(use_pattern='*', use_encoding='utf-8')
        assert 'blarg' in rh_keys, f'Could not find set key "blarg" in keys. Got: {rh_keys}'
        with self.rh.wrapped_redis(op_name='get(blarg)') as r_conn:
            res = float(r_conn.get('blarg'))
        assert res == 5.7, f'Fetched value of "blarg" does not match set value (5.7). Got: {res}'

        print(f'Received matching object back\n{pformat(res, indent=4)}')

        with self.rh.wrapped_redis(op_name='delete(blarg)') as r_conn:
            res = r_conn.delete('blarg')
        assert res > 0, f'Bad return from delete(): {res}'

        print('All regular tests complete')

        with self.rh.wrapped_redis(op_name='hset(beep, boop, ...)') as r_conn:
            res = True if r_conn.hset('beep', 'boop', 40.66) else False
            assert res, f'Bad return from hset(beep, boop, ...): {res} (0 indicates the key already exists..)'

        with self.rh.wrapped_redis(op_name='hexists(beep, boop)') as r_conn:
            res = r_conn.hexists('beep', 'boop')
        assert res, 'Set hash entry "boop" in key "beep" not found after setting.'

        with self.rh.wrapped_redis(op_name='hkeys(beep)') as r_conn:
            res = [val.decode('utf-8') for val in r_conn.hkeys('beep')]
        assert res, 'No hkeys returned for "beep".'
        assert 'boop' in res, f'Could not find hash entry "boop" in key "beep". Got: {res}'

        rh_hkeys = self.rh.keys(redis_id='beep')
        assert 'boop' in rh_hkeys, f'Could not find entry "boop" in key "beep". Got: {rh_hkeys}'

        with self.rh.wrapped_redis(op_name='hget(beep, boop)') as r_conn:
            res = float(r_conn.hget('beep', 'boop'))
        assert res == 40.66, f'Fetched value of "boop" from key "beep" does not match set value (40.66). Got: {res}'

        print(f'Received matching hash entry "boop" from key "beep" back\n{pformat(res, indent=4)}')

        @self.rh.decode_entries(first_handler=lambda res: {k.decode('utf-8'): float(v) for k, v in res.items()})
        def get_all():
            with self.rh.wrapped_redis(op_name='hgetall(beep)') as r_conn:
                return r_conn.hgetall('beep')

        all_ents = get_all()
        assert 'boop' in all_ents, f'Missing "boop" entry in hgetall keys: "{all_ents.keys()}"'
        assert all_ents == {'boop': 40.66}, f'Expected single dictionary entry in hgetall result. Got: {all_ents}'

        print(f'Received full hash entry for "beep":\n{pformat(all_ents, indent=4)}')

        with self.rh.wrapped_redis(op_name='hdel(beep, boop)') as r_conn:
            res = r_conn.hdel('beep', 'boop')
        assert res > 0, f'Bad return from hdel(beep, boop): {res}'

        print('All hash tests complete')

    '''
    def test_bad_sync_redis_value(self):
        r_pool = RedisentHelper.build_pool(redis_uri='localhost')
        rh = RedisentHelper(r_pool)

        with pytest.raises(RedisError):
            with self.rh.wrapped_redis(op_name='set(bad_val, ...)') as r_conn:
                r_conn.set('bad_val', {'one': 1, 'oh_no': datetime.now()})

    def test_redis_error():
        try:
            raise RedisError('Oh what a dumb error', related_command=None, extra_attrs={'value': 5, 'ts': datetime.now()})
        except RedisError as ex:
            print(f'Caught expected dummy RedisError: "{ex}"')
            print(f'repr(ex):\n{repr(ex)}')
            print(f'Dumping Exception:\n{ex.dump()}')
        except Exception as ex:
            pytest.fail(f'Received un-expected exception instead of "RedisError": {ex}', True)


    def test_helper_methods_sync(self):
        r_pool = RedisentHelper.build_pool(redis_uri='localhost')
        rh = RedisentHelper(r_pool)

        with self.rh.wrapped_redis(op_name='hset(testing, value, 1)') as r_conn:
            r_conn.hset('testing', 'value', 1)

        assert self.rh.exists('testing'), 'Cannot find newly set "testing" key'

        found_keys = self.rh.keys(use_pattern='test*')

        assert b'testing' in found_keys, f'Cannot find newly set key "testing" in lookup of "test*". Found: "{found_keys}"'


    def test_decode_decorator():
        dt_now = datetime.now()
        raw_data = {'attr_one': 1, 'attr_two': 2, 'dt': dt_now, 'more': [1, 2, 5.5]}
        enc_data = {'non_encoded': 'yes', 'nested': pickle.dumps(raw_data), b'encoded_key': b'encoded_value'}

        handler_ran = False

        def decode_handler(value):
            global handler_ran
            print(f'In decode handler with "{value}"')
            handler_ran = True

            return value

        @RedisentHelper.decode_entries(use_encoding='utf-8', final_handler=decode_handler)
        def dummy_fetch():
            return enc_data

        res = dummy_fetch()

        assert res['nested'] == raw_data, f'Nested pickle data does not match.\nExpected: {raw_data}\nGot: {res["nested"]}'

        enc_res = res.get('encoded_key', None)
        if not enc_res:
            assert b'encoded_key' not in res, 'Found no attribute in results for "encoded_key" (checked both str and byte types)'

            assert False, 'Found byte-type key "encoded_key" in results that was not decoxed. Got: {enc_res}'

        assert enc_res == b'encoded_value', f'Found encoded key in results but wrong value. Should be b"encoded_value" instead of {enc_res}'

        print('Encoded attribute for "encoded_key" found in results with value b"encoded_key"')
    '''
