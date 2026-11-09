"""Exercise duplicate-key recovery and infrastructure failure boundaries."""

from datetime import UTC, datetime

import pytest
from pymongo.errors import AutoReconnect, DuplicateKeyError

from call_service.exceptions import CampaignConflictError
from call_service.mongo import MongoRepository
from call_service.schemas import Campaign


class RacingCollection:
    def __init__(self, existing: dict[str, object] | None) -> None:
        self.existing = existing

    async def update_one(self, *_args: object, **_kwargs: object) -> None:
        raise DuplicateKeyError("E11000 duplicate key")

    async def find_one(self, _query: object) -> dict[str, object] | None:
        return self.existing


def campaign() -> Campaign:
    return Campaign(
        campaign_id="camp_001",
        name="Campaign",
        created_at=datetime(2026, 10, 20, 10, 0, 0, 123456, tzinfo=UTC),
    )


def repository_with_race(existing: dict[str, object] | None) -> MongoRepository:
    repository = object.__new__(MongoRepository)
    repository.campaigns = RacingCollection(existing)  # type: ignore[assignment]
    return repository


@pytest.mark.asyncio
async def test_duplicate_key_with_same_document_is_success() -> None:
    existing = {
        "campaign_id": "camp_001",
        "name": "Campaign",
        "created_at": datetime(2026, 10, 20, 10, 0, 0, 123000),
    }
    await repository_with_race(existing).ensure_campaign(campaign())


@pytest.mark.asyncio
async def test_duplicate_key_with_conflicting_document_is_permanent() -> None:
    existing = {
        "campaign_id": "camp_001",
        "name": "Other campaign",
        "created_at": datetime(2026, 10, 20, 10, 0, 0, 123000),
    }
    with pytest.raises(CampaignConflictError):
        await repository_with_race(existing).ensure_campaign(campaign())


@pytest.mark.asyncio
async def test_duplicate_key_without_matching_document_is_not_suppressed() -> None:
    with pytest.raises(DuplicateKeyError):
        await repository_with_race(None).ensure_campaign(campaign())


@pytest.mark.asyncio
async def test_infrastructure_error_is_not_changed_to_permanent_error() -> None:
    class UnavailableCollection:
        async def update_one(self, *_args: object, **_kwargs: object) -> None:
            raise AutoReconnect("MongoDB unavailable")

    repository = object.__new__(MongoRepository)
    repository.campaigns = UnavailableCollection()  # type: ignore[assignment]
    with pytest.raises(AutoReconnect, match="MongoDB unavailable"):
        await repository.ensure_campaign(campaign())
