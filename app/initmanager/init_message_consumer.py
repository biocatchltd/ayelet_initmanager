import logging
from typing import Any, Dict

import orjson
from aio_pika import IncomingMessage
from azure.storage.blob.aio import BlobServiceClient

from app.utils import blob

logger = logging.getLogger('biocatch.' + __name__)


async def handle_rmq_message(incoming_message: IncomingMessage,  container: str, profiles_pref: str,
                             ds_profiles_pref: str, blob_service_client: BlobServiceClient, redisinst)\
        -> None:
    try:
        parsed_message: Dict[str, Any] = orjson.loads(incoming_message.body)
        uid = parsed_message['uid']
    except Exception:
        logger.exception('failed to parse incoming message', extra={'message': incoming_message})
    try:
        blob_profiles = await blob.download_user_data(uid, container, profiles_pref, ds_profiles_pref,
                                                      blob_service_client)
        await redisinst.hmset(name=uid, mapping=blob_profiles)
    except Exception:
        logger.exception('Failed to load profiles', extra={'uid': uid})
        return
