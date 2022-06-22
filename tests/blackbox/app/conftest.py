import logging
from dataclasses import asdict

import envolved
from async_asgi_testclient import TestClient
from azure.storage.blob import BlobServiceClient
from pydantic import BaseModel
from pytest import MonkeyPatch, fixture

from tests.blackbox.conftest import BlackboxEnv

logger = logging.getLogger('biocatch.' + __name__)


class InitMessage(BaseModel):
    uid: str


@fixture
def setup_env(env_vars: BlackboxEnv, redis, rabbitmq, blob_storage) -> BlackboxEnv:
    env_vars.redis_host = '127.0.0.1'
    env_vars.redis_port = str(redis.client_port())
    env_vars.rabbitmq_host = '127.0.0.1'
    env_vars.rabbitmq_port = str(rabbitmq.connection_port())
    env_vars.rabbitmq_queue_username = "guest"
    env_vars.rabbitmq_queue_password = "guest"
    env_vars.rabbitmq_exchange = "initmanagerblackbox"
    env_vars.rabbitmq_queue_name = "initmanagerblackbox_init_manager"
    env_vars.connection_string = blob_storage.connection_string
    env_vars.container_name = "training"
    env_vars.profiles_prefix = "profile-uid-"
    env_vars.ds_profiles_prefix = "ds-profile-uid-"

    return env_vars


@fixture
def storage_client(blob_storage):
    with BlobServiceClient.from_connection_string(blob_storage.connection_string) as client:
        container_name: envolved.EnvVar[str] = \
            envolved.env_var('container_name', type=str)
        name = container_name.get()
        client.create_container(name)
        yield client


@fixture
async def initmanager_client(env_name: str, setup_env: BlackboxEnv, redis, rabbitmq, blob_storage,
                             monkeypatch: MonkeyPatch) -> TestClient:
    d = asdict(setup_env)
    for key in d:
        monkeypatch.setenv(key, d[key])

    from app.main import app

    async with TestClient(app) as client:
        yield client
