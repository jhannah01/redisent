import pytest
import aioredis

import fakeredis
import fakeredis.aioredis
import redis

pytestmark = [pytest.mark.asyncio]


@pytest.fixture()
def use_fake_aioredis(mocker):
    mocker.patch.object(aioredis, 'ConnectionsPool', new=fakeredis.aioredis.FakeConnectionsPool)


@pytest.fixture()
def use_fake_redis(mocker):
    mocker.patch.object(redis, 'StrictRedis', new=fakeredis.FakeStrictRedis)
