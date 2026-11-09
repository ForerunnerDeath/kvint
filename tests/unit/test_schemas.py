"""Event contract checks that require no database."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from call_service.exceptions import InvalidEventError
from call_service.handler import handle_event
from call_service.schemas import CallEvent


def event_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "campaign": {
            "campaign_id": "camp_001",
            "name": "Напоминание",
            "created_at": "2026-10-20T13:00:00+03:00",
        },
        "call": {
            "call_id": "call_001",
            "campaign_id": "camp_001",
            "phone": "79001234567",
            "status": "busy",
            "created_at": "2026-10-20T13:06:00+03:00",
        },
    }


def test_valid_event_normalizes_utc_and_accepts_arbitrary_status() -> None:
    event = CallEvent.model_validate(event_payload())
    assert event.campaign.created_at == datetime(2026, 10, 20, 10, tzinfo=UTC)
    assert event.call.created_at == datetime(2026, 10, 20, 10, 6, tzinfo=UTC)
    assert event.call.status == "busy"


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("schema_version",), 2),
        (("schema_version",), True),
        (("schema_version",), 1.0),
        (("campaign", "campaign_id"), "  "),
        (("campaign", "name"), ""),
        (("call", "call_id"), ""),
        (("call", "phone"), 79001234567),
        (("call", "status"), " "),
        (("campaign", "created_at"), "2026-10-20T10:00:00"),
        (("call", "created_at"), "2026-10-20T10:06:00"),
        (("call", "created_at"), 1792483560),
        (("call", "campaign_id"), "other"),
    ],
)
def test_invalid_events(path: tuple[str, ...], value: object) -> None:
    payload = event_payload()
    target: object = payload
    for key in path[:-1]:
        assert isinstance(target, dict)
        target = target[key]
    assert isinstance(target, dict)
    target[path[-1]] = value
    with pytest.raises(ValidationError):
        CallEvent.model_validate(payload)


@pytest.mark.parametrize("section", [None, "campaign", "call"])
def test_extra_fields_are_forbidden(section: str | None) -> None:
    payload = event_payload()
    target = payload if section is None else payload[section]
    assert isinstance(target, dict)
    target["unexpected"] = "value"
    with pytest.raises(ValidationError):
        CallEvent.model_validate(payload)


@pytest.mark.parametrize("section", ["campaign", "call"])
def test_missing_required_fields(section: str) -> None:
    payload = event_payload()
    target = payload[section]
    assert isinstance(target, dict)
    target.pop("created_at")
    with pytest.raises(ValidationError):
        CallEvent.model_validate(payload)


@pytest.mark.asyncio
async def test_validation_happens_before_any_write() -> None:
    class SpyRepository:
        def __init__(self) -> None:
            self.writes: list[str] = []

        async def ensure_campaign(self, _campaign: object) -> None:
            self.writes.append("campaign")

        async def ensure_call(self, _call: object) -> None:
            self.writes.append("call")

    repository = SpyRepository()
    payload = deepcopy(event_payload())
    call = payload["call"]
    assert isinstance(call, dict)
    call["campaign_id"] = "other"
    with pytest.raises(InvalidEventError):
        await handle_event(payload, repository)  # type: ignore[arg-type]
    assert repository.writes == []
