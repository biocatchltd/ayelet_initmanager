import logging
import zlib

import ormsgpack
from aio_pika import IncomingMessage
from envolved import EnvVar, env_var
from fastapi import FastAPI
from redis.asyncio import Redis

from app.initmanager.init_message_consumer import handle_rmq_message
from app.utils import rabbitmq
from app.utils.blob import create_blob_client

logger = logging.getLogger('biocatch.ayelet_init_manager')
TIMEOUT = 10000  # ms


async def create_connection_pool():
    redis_host: EnvVar[str] = env_var('redis_host', type=str)
    redis_port: EnvVar[str] = env_var('redis_port', type=str)
    host = redis_host.get()
    port = redis_port.get()
    return await Redis(host=host, port=port)


class InitManager(FastAPI):

    async def prepare(self) -> None:
        try:
            profiles_prefix: EnvVar[str] = env_var('profiles_prefix', type=str)
            self.profiles_pref = profiles_prefix.get()
            ds_profiles_prefix: EnvVar[str] = env_var('ds_profiles_prefix', type=str)
            self.ds_profiles_pref = ds_profiles_prefix.get()
            container_name: EnvVar[str] = env_var('container_name', type=str)
            self.container = container_name.get()
            self.blob = create_blob_client()
            self.redis = await create_connection_pool()
            self._rabbitmq_consumer = await rabbitmq.init_rabbitmq_consumer(self.consume_init)
            await self._rabbitmq_consumer.start_consuming()
        except Exception:
            logger.exception('exception when preparing init manager environment')
            raise

    async def close(self) -> None:
        await self._rabbitmq_consumer.close()
        self.redis.close()
        await self.blob.close()

    async def consume_init(self, incoming_message: IncomingMessage) -> None:
        await handle_rmq_message(incoming_message, self.container, self.profiles_pref, self.ds_profiles_pref,
                                 self.blob, self.redis)


app = InitManager()


@app.on_event('startup')
async def startup_event() -> None:
    await app.prepare()


@app.on_event('shutdown')
async def shutdown_event() -> None:
    await app.close()


@app.get("/api/v1/get-data")
async def get_data(uid: str):
    profiles = await app.redis.hgetall(uid)
    if (profiles == {}):
        return 'user data missing'
    profiles_decoded = {key.decode(): profiles.get(key) for key in profiles.keys()}
    decompressed_profile = zlib.decompress(profiles_decoded[get_key(app.profiles_pref, uid)])
    decompressed_ds_profile = zlib.decompress(profiles_decoded[get_key(app.ds_profiles_pref, uid)])
    deserialized_profile = ormsgpack.unpackb(decompressed_profile)
    deserialized_ds_profile = ormsgpack.unpackb(decompressed_ds_profile)
    return deserialized_profile, deserialized_ds_profile


@app.get("/api/readiness")
async def ready() -> str:
    return "I'm ready !!!!!"


def get_key(prefix: str, uid: str) -> str:
    return prefix + uid
