import logging
import zlib

import aioredis
import envolved
import ormsgpack
from aioredis import Redis

logger = logging.getLogger('biocatch.' + __name__)
TIMEOUT = 5000  # ms


async def create_connection_pool() -> Redis:
    try:
        redis_host: envolved.EnvVar[str] = \
            envolved.env_var('redis_host', type=str)
        redis_port: envolved.EnvVar[str] = \
            envolved.env_var('redis_port', type=str)
        host = redis_host.get()
        port = redis_port.get()
        return await aioredis.create_redis_pool(
            address=(host, port), timeout=TIMEOUT)
    except Exception:
        logger.exception("Redis connection failed")
        raise


async def hsetnx(key: str, field: str, value: bytes, redis: Redis) -> None:
    await redis.hsetnx(key, field, value)


async def hget(key: str, field: str, redis: Redis) -> bytes:
    profile = await redis.hget(key, field)
    decompressed_profile = zlib.decompress(profile)
    deserialized_profile = ormsgpack.unpackb(decompressed_profile)
    return deserialized_profile
