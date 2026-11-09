"""Campaign isolation and ordering against a real isolated MongoDB database."""

import asyncio
from time import monotonic
from types import SimpleNamespace

import httpx
import pytest
from pymongo import AsyncMongoClient

from call_service.handler import handle_event
from call_service.main import create_app
from call_service.mongo import MongoRepository


@pytest.mark.asyncio
async def test_api_reads_only_requested_campaign_in_order(
    mongo_repository: MongoRepository,
) -> None:
    for campaign_id, call_id, timestamp in (
        ("one", "later", "2026-01-02T00:00:00Z"),
        ("two", "other", "2026-01-01T00:00:00Z"),
        ("one", "earlier", "2026-01-01T00:00:00Z"),
    ):
        await handle_event(
            {
                "schema_version": 1,
                "campaign": {
                    "campaign_id": campaign_id,
                    "name": campaign_id,
                    "created_at": "2026-01-01T00:00:00Z",
                },
                "call": {
                    "call_id": call_id,
                    "campaign_id": campaign_id,
                    "phone": "123",
                    "status": "done",
                    "created_at": timestamp,
                },
            },
            mongo_repository,
        )
    await mongo_repository.campaigns.insert_one(
        {
            "campaign_id": "empty",
            "name": "empty",
            "created_at": "2026-01-01T00:00:00Z",
        }
    )
    app = create_app()
    app.state.repository = mongo_repository
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/campaigns/one/calls")
        assert response.status_code == 200
        assert [call["call_id"] for call in response.json()] == ["earlier", "later"]
        assert all("_id" not in call for call in response.json())
        assert (await client.get("/campaigns/empty/calls")).json() == []
        assert (await client.get("/campaigns/missing/calls")).text == (
            "Кампания не найдена"
        )


@pytest.mark.asyncio
async def test_ready_with_unreachable_mongo_returns_503_quickly() -> None:
    mongo = AsyncMongoClient(
        "mongodb://127.0.0.1:1", serverSelectionTimeoutMS=100
    )
    app = create_app()
    connected = asyncio.Event()
    connected.set()
    app.state.repository = MongoRepository(mongo["call_service_unreachable_test"])
    app.state.rabbit = SimpleNamespace(
        connection=SimpleNamespace(connected=connected, is_closed=False),
        channel=SimpleNamespace(is_closed=False),
    )
    app.state.consumer = SimpleNamespace(is_running=True)
    supervisor = asyncio.create_task(asyncio.sleep(30))
    app.state.supervisor = supervisor
    try:
        start = monotonic()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            response = await client.get("/ready")
        assert response.status_code == 503
        assert monotonic() - start < 3
    finally:
        supervisor.cancel()
        await asyncio.gather(supervisor, return_exceptions=True)
        await mongo.close()
