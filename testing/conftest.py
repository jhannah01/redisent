import aioredis
import pytest
import mockaioredis
import fakeredis.aioredis

pytestmark = [pytest.mark.asyncio]


@pytest.fixture
def fake_server(request):
    server = fakeredis.FakeServer()
    server.connected = request.node.get_closest_marker('disconnected') is None
    return server


@pytest.fixture(autouse=True)
def redis(mocker):
    mocker.patch.object(aioredis, 'create_redis_pool', new=mockaioredis.create_redis_pool)


