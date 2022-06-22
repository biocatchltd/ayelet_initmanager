import asyncio
import logging

import envolved
from azure.storage.blob.aio import BlobServiceClient
from pydantic import typing

logger = logging.getLogger(('biocatch.' + __name__))


def create_blob_client() -> BlobServiceClient:
    connection_string: envolved.EnvVar[str] = \
        envolved.env_var('connection_string', type=str)
    connection = connection_string.get()
    try:
        return BlobServiceClient.from_connection_string(connection)
    except Exception:
        logger.exception(f'blob connection failed')
        raise


def get_blob_name(prefix: str, uid: str) -> str:
    return f'{prefix}{uid}'


async def download_user_data(uid: str, container: str, profiles_pref: str, ds_profiles_pref: str,
                             storage_client: BlobServiceClient) -> typing.Dict[str, bytes]:
    profile_blob_content, ds_profile_blob_content = await asyncio.gather(
        download_blob(container, profiles_pref, storage_client, uid),
        download_blob(container, ds_profiles_pref, storage_client, uid)
    )
    return {get_blob_name(profiles_pref, uid): profile_blob_content,
            get_blob_name(ds_profiles_pref, uid): ds_profile_blob_content}


async def download_blob(container: str, blob_pref: str, storage_client: BlobServiceClient, uid: str) -> bytes:
    profile_blob_client = storage_client.get_blob_client(container=container, blob=get_blob_name(blob_pref, uid))
    async with profile_blob_client:
        blob = await profile_blob_client.download_blob()
        profile_blob_content = await blob.readall()
    return profile_blob_content
