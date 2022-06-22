from unittest.mock import Mock

import aioredis
import pytest
from _pytest.monkeypatch import MonkeyPatch
from aioredis import Redis

from utils import redis

mock_host = 'localhost'
mock_port = '9347'


@pytest.fixture
def aioredis_mock(monkeypatch: MonkeyPatch) -> aioredis:
    monkeypatch.setenv('redis_host', mock_host)
    monkeypatch.setenv('redis_port', mock_port)

    async def create_redis_pool_mock(address, timeout):
        return Redis

    monkeypatch.setattr(aioredis, 'create_redis_pool',
                        Mock(wraps=create_redis_pool_mock))
    return aioredis


@pytest.mark.asyncio
async def test_create_connection_pool(monkeypatch: MonkeyPatch,
                                      aioredis_mock) -> None:
    redis_connection = await redis.create_connection_pool()
    aioredis.create_redis_pool.assert_called_with(
        address=(mock_host, mock_port), timeout=redis.TIMEOUT)
    assert redis_connection
