"""Validate a complete event before writing campaign and call documents."""

from pydantic import ValidationError

from call_service.exceptions import InvalidEventError
from call_service.mongo import MongoRepository
from call_service.schemas import CallEvent


async def handle_event(payload: object, repository: MongoRepository) -> bool:
    """Return whether the call was newly inserted."""
    try:
        event = CallEvent.model_validate(payload)
    except ValidationError as exc:
        raise InvalidEventError("invalid call event") from exc

    await repository.ensure_campaign(event.campaign)
    return await repository.ensure_call(event.call)
