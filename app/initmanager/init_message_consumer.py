import logging
from asyncio import gather
from typing import Any, Dict

import orjson
from aio_pika import IncomingMessage
from aioredis import Redis
from azure.storage.blob.aio import BlobServiceClient

from app.utils import blob

logger = logging.getLogger('biocatch.' + __name__)
MESSAGE_KEY = "uid"


async def handle_rmq_message(incoming_message: IncomingMessage,  container: str, profiles_pref: str,
                             ds_profiles_pref: str, blob_service_client: BlobServiceClient, aioredis: Redis)\
        -> None:
    try:
        parsed_message: Dict[str, Any] = orjson.loads(incoming_message.body)
        uid = parsed_message[MESSAGE_KEY]
    except Exception:
        logger.exception('failed to parse incoming message', extra={'message': incoming_message})
    try:
        blob_profiles = await blob.download_user_data(uid, container, profiles_pref, ds_profiles_pref,
                                                      blob_service_client)
        await gather(*[aioredis.hset(uid, key, blob_profiles[key]) for key in blob_profiles])
    except Exception:
        logger.exception('Failed to load profiles', extra={MESSAGE_KEY: uid})
        return
