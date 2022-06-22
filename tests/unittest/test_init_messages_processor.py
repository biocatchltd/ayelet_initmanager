from unittest.mock import AsyncMock, Mock, patch

import pytest
from aio_pika import IncomingMessage
from aioredis import Redis
from azure.storage.blob.aio import BlobServiceClient

from app.initmanager.init_message_consumer import handle_rmq_message

_uid = "11"
PROF_PREF = "pref"
DS_PROF_PREF = "ds_pref"
PROFILES = {f'{PROF_PREF}{_uid}': b"profile", f'{PROF_PREF}{_uid}': b"ds_profile"}
CONTAINER = "training"


@pytest.fixture
def init_message_1() -> IncomingMessage:
    return Mock(body=b'{"uid": "11"}')


@pytest.fixture
def blob_mock() -> BlobServiceClient:
    return AsyncMock()


@pytest.fixture
def redis_mock() -> Redis:
    return AsyncMock()


@pytest.mark.asyncio
async def test_init_message_handling(init_message_1, blob_mock, redis_mock):
    with patch('utils.blob.download_user_data', return_value=PROFILES) as blob_download_mock:
        with patch('utils.redis.hsetnx', return_value=None) as redis_hsetnx_mock:
            await handle_rmq_message(init_message_1, CONTAINER, PROF_PREF, DS_PROF_PREF, blob_mock, redis_mock)
            blob_download_mock.assert_called_with(_uid, CONTAINER, PROF_PREF, DS_PROF_PREF, blob_mock)
            [redis_hsetnx_mock.assert_called_with(_uid, field, PROFILES[field], redis_mock) for field in PROFILES]
