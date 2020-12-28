import distutils

import aioredis
import pytest
import mockaioredis
import fakeredis.aioredis

from async_generator import yield_, async_generator

pytestmark = [pytest.mark.asyncio]


@pytest.fixture(scope="session")
def is_redis_running():
    try:
        r = redis.StrictRedis('rpi04.synistree.com', port=6379)
        r.ping()
        return True
    except redis.ConnectionError:
        return False
    finally:
        if hasattr(r, 'close'):
            r.close()      # Absent in older versions of redis-py


@pytest.fixture
def fake_server(request):
    server = fakeredis.FakeServer()
    server.connected = request.node.get_closest_marker('disconnected') is None
    return server

@pytest.fixture(
    params=[
        pytest.param('StrictRedis', marks=pytest.mark.real),
        pytest.param('FakeStrictRedis', marks=pytest.mark.fake)
    ]
)
def create_redis(request):
    name = request.param
    if not name.startswith('Fake') and not request.getfixturevalue('is_redis_running'):
        pytest.skip('Redis is not running')
    decode_responses = request.node.get_closest_marker('decode_responses') is not None

    def factory(db=0):
        if name.startswith('Fake'):
            fake_server = request.getfixturevalue('fake_server')
            cls = getattr(fakeredis, name)
            return cls(db=db, decode_responses=decode_responses, server=fake_server)
        else:
            cls = getattr(redis, name)
            conn = cls('localhost', port=6379, db=db, decode_responses=decode_responses)
            min_server_marker = request.node.get_closest_marker('min_server')
            if min_server_marker is not None:
                server_version = conn.info()['redis_version']
                min_version = min_server_marker.args[0]
                if distutils.version.LooseVersion(server_version) < min_version:
                    pytest.skip(
                        'Redis server {} required but {} found'.format(min_version, server_version)
                    )
            return conn

    return factory

@pytest.fixture(
    params=[
        pytest.param('fake', marks=pytest.mark.fake),
        pytest.param('real', marks=pytest.mark.real)
    ]
)
@async_generator
async def r(request):
    if request.param == 'fake':
        ret = await fakeredis.aioredis.create_redis_pool()
    else:
        if not request.getfixturevalue('is_redis_running'):
            pytest.skip('Redis is not running')
        ret = await aioredis.create_redis_pool('redis://rpi04.synistree.com')
    await ret.flushall()

    await yield_(ret)

    await ret.flushall()
    ret.close()
    await ret.wait_closed()

@pytest.fixture
@async_generator
async def conn(r):
    """A single connection, rather than a pool."""
    with await r as conn:
        await yield_(conn)


@pytest.fixture(autouse=True)
def redis(mocker):
    mocker.patch.object(aioredis, 'create_redis_pool', new=mockaioredis.create_redis_pool)


