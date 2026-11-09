"""RabbitMQ delivery semantics using isolated quorum queues and MongoDB."""

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from time import monotonic
from uuid import uuid4

import aio_pika
import httpx
import pytest
import pytest_asyncio
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from dotenv import dotenv_values
from pymongo.errors import AutoReconnect

from call_service.config import get_settings
from call_service.consumer import Consumer
from call_service.mongo import MongoRepository
from call_service.rabbit import connect_consumer
from call_service.schemas import CallEvent


@dataclass
class BrokerFixture:
    queue_name: str
    channel: AbstractChannel
    dlq: AbstractQueue

    async def publish(self, body: bytes) -> None:
        result = await self.channel.default_exchange.publish(
            Message(
                body,
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type="application/json",
                message_id=str(uuid4()),
            ),
            routing_key=self.queue_name,
            mandatory=True,
        )
        assert result is not None


@pytest_asyncio.fixture
async def broker() -> AsyncIterator[BrokerFixture]:
    settings = get_settings()
    local = dotenv_values(".env")
    management_url = (
        os.getenv("RABBITMQ_MANAGEMENT_URL")
        or local.get("RABBITMQ_MANAGEMENT_URL")
        or "http://127.0.0.1:15672"
    )
    suffix = uuid4().hex
    queue_name = f"call_service_test_{suffix}"
    dlq_name = f"{queue_name}_dlq"
    dlx_name = f"{queue_name}_dlx"
    policy_name = f"{queue_name}_policy"
    connection: AbstractRobustConnection = await aio_pika.connect_robust(
        settings.rabbitmq_url, timeout=5
    )
    channel: AbstractChannel | None = None
    async with httpx.AsyncClient(
        base_url=management_url,
        auth=(settings.rabbitmq_user, settings.rabbitmq_password.get_secret_value()),
        timeout=5,
    ) as api:
        try:
            channel = await connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            await channel.declare_exchange(dlx_name, ExchangeType.DIRECT, durable=True)
            dlq = await channel.declare_queue(dlq_name, durable=True)
            dlx = await channel.get_exchange(dlx_name)
            await dlq.bind(dlx, routing_key="dead")
            response = await api.put(
                f"/api/policies/%2F/{policy_name}",
                json={
                    "pattern": f"^{queue_name}$",
                    "apply-to": "quorum_queues",
                    "priority": 20,
                    "definition": {
                        "delivery-limit": 5,
                        "delayed-retry-type": "failed",
                        "delayed-retry-min": 200,
                        "delayed-retry-max": 500,
                        "dead-letter-exchange": dlx_name,
                        "dead-letter-routing-key": "dead",
                        "dead-letter-strategy": "at-least-once",
                        "overflow": "reject-publish",
                    },
                },
            )
            response.raise_for_status()
            await channel.declare_queue(
                queue_name, durable=True, arguments={"x-queue-type": "quorum"}
            )
            deadline = monotonic() + 10
            while True:
                response = await api.get(f"/api/queues/%2F/{queue_name}")
                response.raise_for_status()
                if response.json().get("policy") == policy_name:
                    break
                if monotonic() >= deadline:
                    pytest.fail(f"test queue policy not applied: {policy_name}")
                await asyncio.sleep(0.5)
            yield BrokerFixture(queue_name, channel, dlq)
        finally:
            await api.delete(f"/api/policies/%2F/{policy_name}")
            if channel is not None and not connection.is_closed:
                await channel.queue_delete(queue_name)
                await channel.queue_delete(dlq_name)
                await channel.exchange_delete(dlx_name)
            await connection.close()


def event_body(*, status: str = "success", call_id: str = "test_call") -> bytes:
    event = CallEvent.model_validate(
        {
            "schema_version": 1,
            "campaign": {
                "campaign_id": "test_campaign",
                "name": "Test campaign",
                "created_at": "2026-10-20T10:00:00Z",
            },
            "call": {
                "call_id": call_id,
                "campaign_id": "test_campaign",
                "phone": "79001234567",
                "status": status,
                "created_at": "2026-10-20T10:06:00Z",
            },
        }
    )
    return event.model_dump_json().encode()


async def wait_for_call(repository: MongoRepository, count: int = 1) -> None:
    deadline = monotonic() + 8
    while monotonic() < deadline:
        if await repository.calls.count_documents({}) == count:
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"expected {count} calls before timeout")


async def wait_for_dlq(
    dlq: AbstractQueue, timeout: float = 8
) -> AbstractIncomingMessage:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        message = await dlq.get(no_ack=False, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    pytest.fail("expected dead-lettered message before timeout")


@pytest.mark.asyncio
async def test_invalid_conflict_and_following_valid_event(
    broker: BrokerFixture, mongo_repository: MongoRepository
) -> None:
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, mongo_repository)
    try:
        await consumer.start()
        await broker.publish(b"{invalid")
        await broker.publish(b'{"schema_version":2}')
        await broker.publish(event_body())
        await wait_for_call(mongo_repository)
        first = await wait_for_dlq(broker.dlq)
        second = await wait_for_dlq(broker.dlq)
        await first.ack()
        await second.ack()

        await broker.publish(event_body(status="conflicting"))
        third = await wait_for_dlq(broker.dlq)
        await third.ack()
        assert await mongo_repository.calls.count_documents({}) == 1
        assert not consumer.failed.done()
    finally:
        await consumer.stop()
        await rabbit.close()


class IntermittentRepository:
    def __init__(self, actual: MongoRepository, *, always_fail: bool = False) -> None:
        self.actual = actual
        self.always_fail = always_fail
        self.attempts: list[float] = []

    async def ensure_campaign(self, campaign: object) -> bool:
        return await self.actual.ensure_campaign(campaign)  # type: ignore[arg-type]

    async def ensure_call(self, call: object) -> bool:
        self.attempts.append(monotonic())
        if self.always_fail or len(self.attempts) == 1:
            raise AutoReconnect("simulated transient write failure")
        return await self.actual.ensure_call(call)  # type: ignore[arg-type]

    async def ping(self) -> None:
        await self.actual.ping()


@pytest.mark.asyncio
async def test_native_delayed_retry_and_ack_after_write(
    broker: BrokerFixture, mongo_repository: MongoRepository
) -> None:
    repository = IntermittentRepository(mongo_repository)
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, repository)  # type: ignore[arg-type]
    try:
        await consumer.start()
        await broker.publish(event_body())
        await wait_for_call(mongo_repository)
        assert len(repository.attempts) == 2
        assert repository.attempts[1] - repository.attempts[0] >= 0.18
        assert not consumer.failed.done()
    finally:
        await consumer.stop()
        await rabbit.close()


@pytest.mark.asyncio
async def test_delivery_limit_routes_poison_message_to_dlq(
    broker: BrokerFixture, mongo_repository: MongoRepository
) -> None:
    repository = IntermittentRepository(mongo_repository, always_fail=True)
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, repository)  # type: ignore[arg-type]
    try:
        await consumer.start()
        await broker.publish(event_body())
        dead = await wait_for_dlq(broker.dlq, timeout=12)
        assert len(repository.attempts) == 6
        assert dead.headers is not None
        deaths = dead.headers["x-death"]
        assert isinstance(deaths, list) and deaths
        assert isinstance(deaths[0], dict)
        assert deaths[0]["reason"] == "delivery_limit"
        assert await mongo_repository.calls.count_documents({}) == 0
        await dead.ack()
    finally:
        await consumer.stop()
        await rabbit.close()


@pytest.mark.asyncio
async def test_consumer_stop_stops_new_deliveries(
    broker: BrokerFixture, mongo_repository: MongoRepository
) -> None:
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, mongo_repository)
    await consumer.start()
    await consumer.stop()
    await broker.publish(event_body())
    await asyncio.sleep(0.2)
    assert await mongo_repository.calls.count_documents({}) == 0
    await rabbit.close()


@pytest.mark.asyncio
async def test_repeated_delivery_creates_no_duplicate(
    broker: BrokerFixture,
    mongo_repository: MongoRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = mongo_repository.ensure_call
    attempts = 0
    twice = asyncio.Event()

    async def counted(call: object) -> bool:
        nonlocal attempts
        result = await original(call)  # type: ignore[arg-type]
        attempts += 1
        if attempts == 2:
            twice.set()
        return result

    monkeypatch.setattr(mongo_repository, "ensure_call", counted)
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, mongo_repository)
    try:
        await consumer.start()
        await broker.publish(event_body())
        await broker.publish(event_body())
        await asyncio.wait_for(twice.wait(), timeout=8)
        assert await mongo_repository.calls.count_documents({}) == 1
        assert not consumer.failed.done()
    finally:
        await consumer.stop()
        await rabbit.close()


@pytest.mark.asyncio
async def test_global_outage_holds_delivery_without_exhausting_limit(
    broker: BrokerFixture, mongo_repository: MongoRepository
) -> None:
    class OutageRepository:
        def __init__(self) -> None:
            self.available = False
            self.first_attempt = asyncio.Event()

        async def ensure_campaign(self, campaign: object) -> bool:
            self.first_attempt.set()
            if not self.available:
                raise AutoReconnect("simulated full MongoDB outage")
            return await mongo_repository.ensure_campaign(campaign)  # type: ignore[arg-type]

        async def ensure_call(self, call: object) -> bool:
            return await mongo_repository.ensure_call(call)  # type: ignore[arg-type]

        async def ping(self) -> None:
            if not self.available:
                raise AutoReconnect("simulated full MongoDB outage")
            await mongo_repository.ping()

    repository = OutageRepository()
    rabbit = await connect_consumer(get_settings(), queue_name=broker.queue_name)
    consumer = Consumer(rabbit, repository, recovery_interval=0.1)  # type: ignore[arg-type]
    try:
        await consumer.start()
        await broker.publish(event_body())
        await broker.publish(event_body(call_id="second_call"))
        await asyncio.wait_for(repository.first_attempt.wait(), timeout=5)
        await asyncio.sleep(0.7)
        assert await mongo_repository.calls.count_documents({}) == 0
        assert await broker.dlq.get(no_ack=False, fail=False) is None
        repository.available = True
        await wait_for_call(mongo_repository, count=2)
        assert not consumer.failed.done()
    finally:
        await consumer.stop()
        await rabbit.close()
