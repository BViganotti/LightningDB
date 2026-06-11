"""Tests for the async LightningDB HTTP client."""
from __future__ import annotations

import pytest

from lightning.client import AsyncClient, ClientConfig, RetryConfig


@pytest.fixture
def client() -> AsyncClient:
    return AsyncClient(ClientConfig(base_url="http://localhost:9999", retry=RetryConfig(max_retries=0)))


@pytest.mark.asyncio
async def test_async_client_context_manager(client: AsyncClient) -> None:
    async with client as c:
        assert c is client


@pytest.mark.asyncio
async def test_async_store_validates(client: AsyncClient) -> None:
    with pytest.raises(ValueError):
        await client.store("", "content")
