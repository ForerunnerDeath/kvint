"""Publish the four sample call events from the host."""

import asyncio
from uuid import uuid4

import aio_pika
import structlog
from aio_pika import DeliveryMode, Message

from call_service.config import get_settings
from call_service.log_config import configure_logging
from call_service.rabbit import CALLS_QUEUE
from call_service.schemas import CallEvent


def sample_events() -> list[CallEvent]:
    campaigns = (
        ("camp_001", "Напоминание о платеже"),
        ("camp_002", "Подтверждение заказа"),
    )
    events: list[CallEvent] = []
    for campaign_id, name in campaigns:
        for number, status in ((1, "success"), (2, "failed")):
            events.append(
                CallEvent.model_validate(
                    {
                        "schema_version": 1,
                        "campaign": {
                            "campaign_id": campaign_id,
                            "name": name,
                            "created_at": "2026-10-20T10:00:00Z",
                        },
                        "call": {
                            "call_id": f"{campaign_id}_call_{number:04d}",
                            "campaign_id": campaign_id,
                            "phone": f"790012345{number:02d}",
                            "status": status,
                            "created_at": f"2026-10-20T10:0{number}:00Z",
                        },
                    }
                )
            )
    return events


async def publish_events() -> None:
    settings = get_settings()
    log = structlog.get_logger().bind(component="producer")
    connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=5)
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await channel.declare_queue(CALLS_QUEUE, passive=True)
        for event in sample_events():
            message_id = str(uuid4())
            message = Message(
                body=event.model_dump_json().encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=message_id,
            )
            confirmation = await channel.default_exchange.publish(
                message, routing_key=CALLS_QUEUE, mandatory=True, timeout=10
            )
            if confirmation is None:
                raise RuntimeError("publisher confirm was not received")
            log.info(
                "event_published",
                message_id=message_id,
                call_id=event.call.call_id,
                campaign_id=event.campaign.campaign_id,
            )
    finally:
        await connection.close()


def main() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json_logs=settings.log_json)
    try:
        asyncio.run(publish_events())
    except Exception as exc:
        structlog.get_logger().error(
            "publication_failed", error_type=type(exc).__name__
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
