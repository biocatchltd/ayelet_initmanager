from unittest.mock import MagicMock

import pytest
from azure.storage.blob.aio import BlobServiceClient

from app.utils import blob

connection_string = \
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;' \
    'AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;' \
    'BlobEndpoint=http://localhost:55013/devstoreaccount1;'


@pytest.fixture
def blob_mock(monkeypatch) -> BlobServiceClient:
    monkeypatch.setenv('connection_string', connection_string)

    def from_connection_string(connection_string):
        return None

    monkeypatch.setattr(BlobServiceClient, 'from_connection_string',
                        MagicMock())
    return BlobServiceClient


@pytest.mark.asyncio
async def test_create_blob_client(monkeypatch, blob_mock) -> None:
    blob.create_blob_client()
    BlobServiceClient.from_connection_string.assert_called_with(connection_string)
