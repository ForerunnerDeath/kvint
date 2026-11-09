"""Read-only HTTP endpoints backed by the lifespan repository."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse

from call_service.mongo import MongoRepository
from call_service.schemas import Call

router = APIRouter()


def get_repository(request: Request) -> MongoRepository:
    return request.app.state.repository  # type: ignore[no-any-return]


@router.get(
    "/campaigns/{campaign_id}/calls",
    response_model=list[Call],
    responses={
        200: {
            "description": "Calls as JSON, or text when the campaign is absent",
            "content": {
                "text/plain": {
                    "schema": {"type": "string", "example": "Кампания не найдена"}
                }
            },
        }
    },
)
async def get_campaign_calls(
    campaign_id: str,
    repository: Annotated[MongoRepository, Depends(get_repository)],
) -> list[Call] | PlainTextResponse:
    if not await repository.campaign_exists(campaign_id):
        return PlainTextResponse("Кампания не найдена")
    return await repository.get_campaign_calls(campaign_id)
