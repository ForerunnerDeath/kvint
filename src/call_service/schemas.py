"""Validated incoming event contract."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)

NonEmptyString = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)
]


class Campaign(BaseModel):
    model_config = ConfigDict(extra="forbid")

    campaign_id: NonEmptyString
    name: NonEmptyString
    created_at: AwareDatetime

    @field_validator("created_at", mode="before")
    @classmethod
    def require_datetime_value(cls, value: object) -> object:
        if not isinstance(value, datetime | str):
            raise ValueError("created_at must be an ISO 8601 string or datetime")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_datetime(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class Call(BaseModel):
    model_config = ConfigDict(extra="forbid")

    call_id: NonEmptyString
    campaign_id: NonEmptyString
    phone: NonEmptyString
    status: NonEmptyString
    created_at: AwareDatetime

    @field_validator("created_at", mode="before")
    @classmethod
    def require_datetime_value(cls, value: object) -> object:
        if not isinstance(value, datetime | str):
            raise ValueError("created_at must be an ISO 8601 string or datetime")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_datetime(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class CallEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    campaign: Campaign
    call: Call

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value

    @model_validator(mode="after")
    def matching_campaign_ids(self) -> "CallEvent":
        if self.campaign.campaign_id != self.call.campaign_id:
            raise ValueError("campaign.campaign_id must equal call.campaign_id")
        return self
