import logging
import zlib
from asyncio import gather
from typing import Tuple

import aioredis
import envolved
import ormsgpack
import uvicorn
from aio_pika import IncomingMessage
from aioredis import Redis
from fastapi import FastAPI

from app.initmanager.init_message_consumer import handle_rmq_message
from app.utils import rabbitmq
from app.utils.blob import create_blob_client

logger = logging.getLogger('biocatch.' + __name__)
TIMEOUT = 10000  # ms


async def create_connection_pool() -> Redis:
    redis_host: envolved.EnvVar[str] = \
        envolved.env_var('redis_host', type=str)
    redis_port: envolved.EnvVar[str] = \
        envolved.env_var('redis_port', type=str)
    host = redis_host.get()
    port = redis_port.get()
    return await aioredis.create_redis_pool(
        address=(host, port), timeout=TIMEOUT)


class InitManager(FastAPI):

    async def prepare(self) -> None:
        try:
            profiles_prefix: envolved.EnvVar[str] = \
                envolved.env_var('profiles_prefix', type=str)
            self.profiles_pref = profiles_prefix.get()
            ds_profiles_prefix: envolved.EnvVar[str] = \
                envolved.env_var('ds_profiles_prefix', type=str)
            self.ds_profiles_pref = ds_profiles_prefix.get()
            container_name: envolved.EnvVar[str] = \
                envolved.env_var('container_name', type=str)
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
        await self.redis.wait_closed()
        await self.blob.close()

    async def consume_init(self, incoming_message: IncomingMessage) -> None:
        try:
            await handle_rmq_message(incoming_message, self.container, self.profiles_pref, self.ds_profiles_pref,
                                     self.blob, self.redis)
        except Exception:
            logger.exception('exception when processing init message on rabbit mq')


app = InitManager()


@app.on_event('startup')
async def startup_event() -> None:
    await app.prepare()


@app.on_event('shutdown')
async def shutdown_event() -> None:
    await app.close()


@app.get("/api/v1/get-data")
async def get_data(uid: str) -> Tuple[bytes, bytes]:
    profile, ds_profile = await gather(get(uid, get_key(app.profiles_pref, uid), app.redis),
                                       get(uid, get_key(app.ds_profiles_pref, uid), app.redis))
    return profile, ds_profile


async def get(key: str, field: str, redis: Redis) -> bytes:
    profile = await redis.hget(key, field)
    decompressed_profile = zlib.decompress(profile)
    deserialized_profile = ormsgpack.unpackb(decompressed_profile)
    return deserialized_profile


@app.get("/api/readiness")
async def ready() -> str:
    return "I'm ready !!!!!"


def get_key(prefix: str, uid: str) -> str:
    return f'{prefix}{uid}'


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port='8001')
