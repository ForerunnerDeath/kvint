"""Producer contract and delivery actions without external services."""

import asyncio
import json
from types import SimpleNamespace

import producer as producer_module
import pytest
from aio_pika import DeliveryMode
from producer import sample_events
from pymongo.errors import AutoReconnect

from call_service.consumer import Consumer
from call_service.exceptions import CallConflictError


def event_body() -> bytes:
    return sample_events()[0].model_dump_json().encode()


class Message:
    def __init__(self, body: bytes, actions: list[str]) -> None:
        self.body = body
        self.actions = actions
        self.message_id = "unit-message"
        self.headers: dict[str, int] = {}

    async def ack(self) -> None:
        self.actions.append("ack")

    async def reject(self, *, requeue: bool) -> None:
        self.actions.append(f"reject:{requeue}")


class Repository:
    def __init__(self, actions: list[str], error: Exception | None = None) -> None:
        self.actions = actions
        self.error = error
        self.fail_count = 0
        self.available = True

    async def ensure_campaign(self, _campaign: object) -> bool:
        self.actions.append("campaign")
        return True

    async def ensure_call(self, _call: object) -> bool:
        self.actions.append("call")
        if self.error is not None:
            self.fail_count += 1
            raise self.error
        return True

    async def ping(self) -> None:
        if not self.available:
            raise AutoReconnect("unavailable")


def consumer(repository: Repository) -> Consumer:
    return Consumer(object(), repository, recovery_interval=0.01)  # type: ignore[arg-type]


def test_producer_has_exactly_four_valid_events() -> None:
    events = sample_events()
    assert len(events) == 4
    assert [event.campaign.campaign_id for event in events] == [
        "camp_001", "camp_001", "camp_002", "camp_002"
    ]
    assert len({event.call.call_id for event in events}) == 4
    assert all("message_id" not in event.model_dump() for event in events)


@pytest.mark.asyncio
async def test_publisher_properties_and_connection_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[object, str, bool]] = []

    class Exchange:
        async def publish(
            self, message: object, *, routing_key: str, mandatory: bool, timeout: int
        ) -> object:
            published.append((message, routing_key, mandatory))
            return object()

    class Channel:
        default_exchange = Exchange()

        async def declare_queue(self, name: str, *, passive: bool) -> None:
            assert name == "calls.queue" and passive

    class Connection:
        closed = False

        async def channel(
            self, *, publisher_confirms: bool, on_return_raises: bool
        ) -> Channel:
            assert publisher_confirms and on_return_raises
            return Channel()

        async def close(self) -> None:
            self.closed = True

    connection = Connection()

    async def connect(_url: str, *, timeout: int) -> Connection:
        assert timeout == 5
        return connection

    monkeypatch.setattr(producer_module.aio_pika, "connect_robust", connect)
    monkeypatch.setattr(
        producer_module,
        "get_settings",
        lambda: SimpleNamespace(rabbitmq_url="amqp://test/"),
    )
    await producer_module.publish_events()
    assert connection.closed and len(published) == 4
    for message, routing_key, mandatory in published:
        assert routing_key == "calls.queue" and mandatory
        assert message.delivery_mode == DeliveryMode.PERSISTENT  # type: ignore[attr-defined]
        assert message.message_id  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_publisher_closes_connection_on_missing_confirm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Exchange:
        async def publish(self, *_args: object, **_kwargs: object) -> None:
            return None

    class Channel:
        default_exchange = Exchange()

        async def declare_queue(self, *_args: object, **_kwargs: object) -> None:
            return None

    class Connection:
        closed = False

        async def channel(self, **_kwargs: object) -> Channel:
            return Channel()

        async def close(self) -> None:
            self.closed = True

    connection = Connection()

    async def connect(_url: str, **_kwargs: object) -> Connection:
        return connection

    monkeypatch.setattr(producer_module.aio_pika, "connect_robust", connect)
    monkeypatch.setattr(
        producer_module,
        "get_settings",
        lambda: SimpleNamespace(rabbitmq_url="amqp://test/"),
    )
    with pytest.raises(RuntimeError, match="confirm"):
        await producer_module.publish_events()
    assert connection.closed


@pytest.mark.asyncio
async def test_ack_happens_after_both_writes() -> None:
    actions: list[str] = []
    await consumer(Repository(actions))._on_message(Message(event_body(), actions))  # type: ignore[arg-type]
    assert actions == ["campaign", "call", "ack"]


@pytest.mark.parametrize("body", [b"{broken", json.dumps({"bad": 1}).encode()])
@pytest.mark.asyncio
async def test_invalid_message_rejected_without_write(body: bytes) -> None:
    actions: list[str] = []
    await consumer(Repository(actions))._on_message(Message(body, actions))  # type: ignore[arg-type]
    assert actions == ["reject:False"]


@pytest.mark.asyncio
async def test_conflict_rejected_without_ack() -> None:
    actions: list[str] = []
    repository = Repository(actions, CallConflictError("conflict"))
    await consumer(repository)._on_message(Message(event_body(), actions))  # type: ignore[arg-type]
    assert actions == ["campaign", "call", "reject:False"]


@pytest.mark.asyncio
async def test_transient_mongo_failure_requests_retry_without_ack() -> None:
    actions: list[str] = []
    repository = Repository(actions, AutoReconnect("temporary"))
    await consumer(repository)._on_message(Message(event_body(), actions))  # type: ignore[arg-type]
    assert actions == ["campaign", "call", "reject:True"]


@pytest.mark.asyncio
async def test_global_mongo_outage_holds_delivery() -> None:
    actions: list[str] = []
    repository = Repository(actions, AutoReconnect("unavailable"))
    repository.available = False
    handler = consumer(repository)

    async def restore() -> None:
        await asyncio.sleep(0.03)
        repository.available = True
        repository.error = None

    task = asyncio.create_task(restore())
    await handler._on_message(Message(event_body(), actions))  # type: ignore[arg-type]
    await task
    assert actions == ["campaign", "call", "campaign", "call", "ack"]


@pytest.mark.asyncio
async def test_unexpected_error_leaves_message_unacked_and_signals_failure() -> None:
    actions: list[str] = []
    handler = consumer(Repository(actions, RuntimeError("bug")))
    await handler._on_message(Message(event_body(), actions))  # type: ignore[arg-type]
    assert actions == ["campaign", "call"]
    assert isinstance(handler.failed.result(), RuntimeError)
