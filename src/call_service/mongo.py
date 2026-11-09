"""MongoDB indexes and idempotent writes for immutable call results."""

from datetime import UTC, datetime
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import DuplicateKeyError

from call_service.exceptions import CallConflictError, CampaignConflictError
from call_service.schemas import Call, Campaign


def bson_datetime(value: datetime) -> datetime:
    """Represent a datetime as MongoDB does: UTC with millisecond precision."""
    normalized = (
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    )
    return normalized.replace(microsecond=(normalized.microsecond // 1000) * 1000)


class MongoRepository:
    def __init__(self, database: AsyncDatabase[dict[str, Any]]) -> None:
        self.database = database
        self.campaigns = database["campaigns"]
        self.calls = database["calls"]

    async def ping(self) -> None:
        await self.database.command("ping")

    async def campaign_exists(self, campaign_id: str) -> bool:
        return (
            await self.campaigns.find_one(
                {"campaign_id": campaign_id}, {"_id": 1}
            )
            is not None
        )

    async def get_campaign_calls(self, campaign_id: str) -> list[Call]:
        documents = await (
            self.calls.find({"campaign_id": campaign_id}, {"_id": 0})
            .sort([("created_at", 1), ("call_id", 1)])
            .to_list()
        )
        return [Call.model_validate(document) for document in documents]

    async def create_indexes(self) -> None:
        await self.campaigns.create_index("campaign_id", unique=True)
        await self.calls.create_index("call_id", unique=True)
        await self.calls.create_index("campaign_id")

    async def ensure_campaign(self, campaign: Campaign) -> bool:
        expected = {
            "campaign_id": campaign.campaign_id,
            "name": campaign.name,
            "created_at": bson_datetime(campaign.created_at),
        }
        try:
            result = await self.campaigns.update_one(
                {"campaign_id": campaign.campaign_id},
                {"$setOnInsert": expected},
                upsert=True,
            )
        except DuplicateKeyError:
            # A concurrent upsert may have inserted this business ID first.
            existing = await self.campaigns.find_one(
                {"campaign_id": campaign.campaign_id}
            )
            if existing is None:
                raise
        else:
            if result.upserted_id is not None:
                return True
            existing = await self.campaigns.find_one(
                {"campaign_id": campaign.campaign_id}
            )

        if existing is None or not self._same_business_fields(existing, expected):
            raise CampaignConflictError(
                f"campaign_id {campaign.campaign_id!r} has conflicting data"
            )
        return False

    async def ensure_call(self, call: Call) -> bool:
        expected = {
            "call_id": call.call_id,
            "campaign_id": call.campaign_id,
            "phone": call.phone,
            "status": call.status,
            "created_at": bson_datetime(call.created_at),
        }
        try:
            result = await self.calls.update_one(
                {"call_id": call.call_id}, {"$setOnInsert": expected}, upsert=True
            )
        except DuplicateKeyError:
            existing = await self.calls.find_one({"call_id": call.call_id})
            if existing is None:
                raise
        else:
            if result.upserted_id is not None:
                return True
            existing = await self.calls.find_one({"call_id": call.call_id})

        if existing is None or not self._same_business_fields(existing, expected):
            raise CallConflictError(f"call_id {call.call_id!r} has conflicting data")
        return False

    @staticmethod
    def _same_business_fields(
        existing: dict[str, Any], expected: dict[str, Any]
    ) -> bool:
        return all(
            (
                bson_datetime(existing[key]) == value
                if key == "created_at" and isinstance(existing.get(key), datetime)
                else existing.get(key) == value
            )
            for key, value in expected.items()
        )
