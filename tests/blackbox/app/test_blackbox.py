import zlib
from asyncio import sleep

import envolved
import pytest
from async_asgi_testclient import TestClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from ormsgpack import ormsgpack
from pika.adapters.blocking_connection import BlockingChannel

from tests.blackbox.app.conftest import InitMessage

_uid = '11'
INIT_MESSAGE_1 = InitMessage(uid=_uid)
BLOB_PROFILE = 'profile_blob_content'
BLOB_DS_PROFILE = 'ds_profile_blob_content'


def create_channel(rabbitmq):
    connection = rabbitmq.connection()
    channel: BlockingChannel = connection.channel()
    rabbitmq_queue_name: envolved.EnvVar[str] = \
        envolved.env_var('rabbitmq_queue_name', type=str)
    queue = rabbitmq_queue_name.get()
    rabbitmq_exchange: envolved.EnvVar[str] = \
        envolved.env_var('rabbitmq_exchange', type=str)
    exchange = rabbitmq_exchange.get()
    channel.queue_declare(queue=queue)
    channel.exchange_declare(exchange=exchange)
    channel.queue_bind(queue, exchange)
    return channel


@pytest.fixture
def rabbit_channel(rabbitmq):
    return create_channel(rabbitmq)


@pytest.fixture
def redis_client(redis):
    return redis.client()


def upload_blob_profile(storage_client: BlobServiceClient):
    container_name: envolved.EnvVar[str] = \
        envolved.env_var('container_name', type=str)
    container = container_name.get()
    container_client: ContainerClient = storage_client.get_container_client(container)
    profiles_prefix: envolved.EnvVar[str] = \
        envolved.env_var('profiles_prefix', type=str)
    profiles_pref = profiles_prefix.get()
    ds_profiles_prefix: envolved.EnvVar[str] = \
        envolved.env_var('ds_profiles_prefix', type=str)
    ds_profiles_pref = ds_profiles_prefix.get()
    upload_blob(container_client, profiles_pref)
    upload_blob(container_client, ds_profiles_pref)


def upload_blob(container_client, blob_prefix):
    ds_profile_blob_client = container_client.get_blob_client(blob=f'{blob_prefix}{_uid}')
    serialized_ds_profile = ormsgpack.packb(BLOB_DS_PROFILE)
    compressed_ds_profile = zlib.compress(serialized_ds_profile)
    ds_profile_blob_client.upload_blob(compressed_ds_profile)


@pytest.mark.asyncio
async def test_verify_data_in_redis_after_init(initmanager_client: TestClient, rabbit_channel, redis_client,
                                               blob_storage, storage_client):
    upload_blob_profile(storage_client)
    send_message(rabbit_channel, INIT_MESSAGE_1)
    await sleep(3)
    resp = await initmanager_client.get(f'/api/v1/get-data?uid={_uid}')
    assert resp.ok
    assert BLOB_PROFILE.encode() in resp.content
    assert BLOB_DS_PROFILE.encode() in resp.content


def send_message(channel: BlockingChannel, message: InitMessage):
    try:
        rabbitmq_queue_name: envolved.EnvVar[str] = \
            envolved.env_var('rabbitmq_queue_name', type=str)
        queue = rabbitmq_queue_name.get()
        rabbitmq_exchange: envolved.EnvVar[str] = \
            envolved.env_var('rabbitmq_exchange', type=str)
        exchange = rabbitmq_exchange.get()
        channel.basic_publish(exchange, queue, message.json())
    except Exception:
        raise
