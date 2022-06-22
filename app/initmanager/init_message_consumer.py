import logging
from asyncio import gather
from typing import Any, Dict

import orjson
from aio_pika import IncomingMessage
from aioredis import Redis
from azure.storage.blob.aio import BlobServiceClient

from utils import blob, redis

logger = logging.getLogger('biocatch.' + __name__)


async def handle_rmq_message(incoming_message: IncomingMessage,  container: str, profiles_pref: str,
                             ds_profiles_pref: str, blob_service_client: BlobServiceClient, aioredis: Redis)\
        -> None:
    try:
        parsed_message: Dict[str, Any] = orjson.loads(incoming_message.body)
        uid = list(parsed_message.values())[0]
    except Exception:
        logger.exception(f'failed to parse incoming message: {incoming_message}')
    try:
        blob_profiles = await blob.download_user_data(uid, container, profiles_pref, ds_profiles_pref,
                                                      blob_service_client)
        await gather(*[redis.hsetnx(uid, key, blob_profiles[key], aioredis) for key in blob_profiles])
    except Exception:
        logger.exception(f'failed to load profiles for uid {uid}')
