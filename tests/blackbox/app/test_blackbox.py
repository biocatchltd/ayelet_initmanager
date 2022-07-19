import zlib
from asyncio import sleep

import pytest
from async_asgi_testclient import TestClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from ormsgpack import ormsgpack
from pika.adapters.blocking_connection import BlockingChannel
from starlette.status import HTTP_404_NOT_FOUND

from tests.blackbox.app.conftest import InitMessage
from tests.blackbox.conftest import BlackboxEnv

_uid = '11'
_not_exist_uid = '12'
INIT_MESSAGE_1 = InitMessage(uid=_uid)
INIT_MESSAGE_2 = InitMessage(uid=_not_exist_uid)
BLOB_PROFILE = 'profile_blob_content'
BLOB_DS_PROFILE = 'ds_profile_blob_content'


@pytest.fixture
def rabbit_channel(env_vars, rabbitmq):
    connection = rabbitmq.connection()
    channel: BlockingChannel = connection.channel()
    rabbit_exchange = env_vars.rabbitmq_exchange
    rabbit_queuename = env_vars.rabbitmq_queue_name
    channel.queue_declare(queue=rabbit_queuename)
    channel.exchange_declare(exchange=rabbit_exchange)
    channel.queue_bind(rabbit_queuename, rabbit_exchange)
    return channel


@pytest.fixture
def redis_client(redis):
    return redis.client()


def upload_blob_profile(env_vars: BlackboxEnv, storage_client: BlobServiceClient):
    container_client: ContainerClient = storage_client.get_container_client(env_vars.container_name)
    upload_blob(container_client, env_vars.profiles_prefix, BLOB_PROFILE)
    upload_blob(container_client, env_vars.ds_profiles_prefix, BLOB_DS_PROFILE)


def upload_blob(container_client, blob_prefix, profile_content):
    profile_blob_client = container_client.get_blob_client(blob=f'{blob_prefix}{_uid}')
    serialized_profile = ormsgpack.packb(profile_content)
    compressed_profile = zlib.compress(serialized_profile)
    profile_blob_client.upload_blob(compressed_profile)


@pytest.mark.asyncio
async def test_verify_data_in_redis_after_init(env_vars: BlackboxEnv, initmanager_client: TestClient,
                                               rabbit_channel, redis_client, blob_storage, storage_client):
    upload_blob_profile(env_vars, storage_client)
    send_message(env_vars, rabbit_channel, INIT_MESSAGE_1)
    await sleep(3)
    resp = await initmanager_client.get(f'/api/v1/get-data?uid={_uid}')
    assert resp.ok
    assert resp.json()['data'] == [BLOB_PROFILE, BLOB_DS_PROFILE]


@pytest.mark.asyncio
async def test_error_on_get_missing_data(env_vars: BlackboxEnv, initmanager_client: TestClient, rabbit_channel,
                                         redis_client, blob_storage, storage_client):
    send_message(env_vars, rabbit_channel, INIT_MESSAGE_2)
    await sleep(3)
    resp = await initmanager_client.get('/api/v1/get-data?uid={_uid}')
    assert resp.status_code == HTTP_404_NOT_FOUND
    assert resp.content.decode() == "no profile retrieved"


def send_message(env_vars: BlackboxEnv, channel: BlockingChannel, message: InitMessage):
    channel.basic_publish(env_vars.rabbitmq_exchange, env_vars.rabbitmq_queue_name, message.json())
