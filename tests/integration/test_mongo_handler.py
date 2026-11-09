"""Idempotence and recovery against a real MongoDB server."""

import asyncio
from copy import deepcopy

import pytest

from call_service.exceptions import (
    CallConflictError,
    CampaignConflictError,
    InvalidEventError,
)
from call_service.handler import handle_event
from call_service.mongo import MongoRepository


def event_payload(
    campaign_id: str = "camp_001", call_id: str = "call_001"
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "campaign": {
            "campaign_id": campaign_id,
            "name": f"Campaign {campaign_id}",
            "created_at": "2026-10-20T13:00:00.123456+03:00",
        },
        "call": {
            "call_id": call_id,
            "campaign_id": campaign_id,
            "phone": "79001234567",
            "status": "busy",
            "created_at": "2026-10-20T13:06:00.987654+03:00",
        },
    }


@pytest.mark.asyncio
async def test_four_calls_two_campaigns_and_indexes(
    mongo_repository: MongoRepository,
) -> None:
    for campaign_id in ("camp_001", "camp_002"):
        for number in (1, 2):
            await handle_event(
                event_payload(campaign_id, f"{campaign_id}_call_{number}"),
                mongo_repository,
            )
    assert await mongo_repository.campaigns.count_documents({}) == 2
    assert await mongo_repository.calls.count_documents({}) == 4
    campaign_indexes = await mongo_repository.campaigns.index_information()
    call_indexes = await mongo_repository.calls.index_information()
    assert campaign_indexes["campaign_id_1"]["unique"] is True
    assert call_indexes["call_id_1"]["unique"] is True
    assert call_indexes["campaign_id_1"]["key"] == [("campaign_id", 1)]


@pytest.mark.asyncio
async def test_repeat_and_bson_datetime_precision(
    mongo_repository: MongoRepository,
) -> None:
    event = event_payload()
    await handle_event(event, mongo_repository)
    again = deepcopy(event)
    campaign = again["campaign"]
    call = again["call"]
    assert isinstance(campaign, dict) and isinstance(call, dict)
    campaign["created_at"] = "2026-10-20T10:00:00.123999Z"
    call["created_at"] = "2026-10-20T10:06:00.987001Z"
    await handle_event(again, mongo_repository)
    assert await mongo_repository.campaigns.count_documents({}) == 1
    assert await mongo_repository.calls.count_documents({}) == 1
    stored = await mongo_repository.calls.find_one({"call_id": "call_001"})
    assert stored is not None
    assert stored["created_at"].microsecond == 987000
    assert stored["created_at"].utcoffset().total_seconds() == 0


@pytest.mark.asyncio
async def test_conflicting_campaign_is_permanent(
    mongo_repository: MongoRepository,
) -> None:
    await handle_event(event_payload(), mongo_repository)
    changed = event_payload(call_id="call_002")
    campaign = changed["campaign"]
    assert isinstance(campaign, dict)
    campaign["name"] = "Different name"
    with pytest.raises(CampaignConflictError):
        await handle_event(changed, mongo_repository)
    assert await mongo_repository.calls.count_documents({}) == 1


@pytest.mark.asyncio
async def test_conflicting_call_is_permanent(mongo_repository: MongoRepository) -> None:
    await handle_event(event_payload(), mongo_repository)
    changed = event_payload()
    call = changed["call"]
    assert isinstance(call, dict)
    call["status"] = "completed"
    with pytest.raises(CallConflictError):
        await handle_event(changed, mongo_repository)
    assert await mongo_repository.calls.count_documents({}) == 1


@pytest.mark.asyncio
async def test_invalid_event_does_not_write(mongo_repository: MongoRepository) -> None:
    event = event_payload()
    call = event["call"]
    assert isinstance(call, dict)
    call["campaign_id"] = "wrong"
    with pytest.raises(InvalidEventError):
        await handle_event(event, mongo_repository)
    assert await mongo_repository.campaigns.count_documents({}) == 0
    assert await mongo_repository.calls.count_documents({}) == 0


@pytest.mark.asyncio
async def test_concurrent_identical_events(mongo_repository: MongoRepository) -> None:
    await asyncio.gather(
        *(handle_event(event_payload(), mongo_repository) for _ in range(12))
    )
    assert await mongo_repository.campaigns.count_documents({}) == 1
    assert await mongo_repository.calls.count_documents({}) == 1


@pytest.mark.asyncio
async def test_concurrent_conflicting_calls(mongo_repository: MongoRepository) -> None:
    first = event_payload()
    second = event_payload()
    call = second["call"]
    assert isinstance(call, dict)
    call["status"] = "other"
    results = await asyncio.gather(
        handle_event(first, mongo_repository),
        handle_event(second, mongo_repository),
        return_exceptions=True,
    )
    assert sum(result is True for result in results) == 1
    assert sum(isinstance(result, CallConflictError) for result in results) == 1
    assert await mongo_repository.calls.count_documents({}) == 1


@pytest.mark.asyncio
async def test_retry_after_failure_between_writes(
    mongo_repository: MongoRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    actual_ensure_call = mongo_repository.ensure_call

    async def fail_once(_call: object) -> None:
        raise RuntimeError("simulated failure after campaign write")

    monkeypatch.setattr(mongo_repository, "ensure_call", fail_once)
    with pytest.raises(RuntimeError, match="simulated failure"):
        await handle_event(event_payload(), mongo_repository)
    assert await mongo_repository.campaigns.count_documents({}) == 1
    assert await mongo_repository.calls.count_documents({}) == 0

    monkeypatch.setattr(mongo_repository, "ensure_call", actual_ensure_call)
    await handle_event(event_payload(), mongo_repository)
    assert await mongo_repository.campaigns.count_documents({}) == 1
    assert await mongo_repository.calls.count_documents({}) == 1
