"""HTTP contract and bounded consumer shutdown without external services."""

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
import structlog

from call_service.consumer import Consumer
from call_service.log_config import configure_logging
from call_service.main import _supervise_consumer, create_app
from call_service.schemas import Call


class ReadRepository:
    def __init__(self) -> None:
        self.available = True
        self.campaigns = {"one", "empty"}
        self.calls = [
            Call(
                call_id="a",
                campaign_id="one",
                phone="123",
                status="done",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        ]

    async def campaign_exists(self, campaign_id: str) -> bool:
        return campaign_id in self.campaigns

    async def get_campaign_calls(self, campaign_id: str) -> list[Call]:
        return [call for call in self.calls if call.campaign_id == campaign_id]

    async def ping(self) -> None:
        if not self.available:
            raise RuntimeError("mongo unavailable")


@pytest.mark.asyncio
async def test_calls_and_openapi_contract() -> None:
    app = create_app()
    app.state.repository = ReadRepository()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/campaigns/one/calls")
        assert response.status_code == 200
        assert len(response.json()) == 1
        assert "_id" not in response.json()[0]
        assert response.json()[0]["created_at"].endswith("Z")
        assert (await client.get("/campaigns/empty/calls")).json() == []
        missing = await client.get("/campaigns/missing/calls")
        assert missing.status_code == 200
        assert missing.headers["content-type"].startswith("text/plain")
        assert missing.text == "Кампания не найдена"
        assert (await client.get("/health")).json() == {"status": "ok"}
        docs = (await client.get("/openapi.json")).json()
        response_docs = docs["paths"]["/campaigns/{campaign_id}/calls"]["get"][
            "responses"
        ]["200"]["content"]
        assert response_docs["application/json"]["schema"]["items"]["$ref"].endswith(
            "/Call"
        )
        assert response_docs["text/plain"]["schema"]["example"] == (
            "Кампания не найдена"
        )


@pytest.mark.asyncio
async def test_readiness_fails_closed() -> None:
    app = create_app()
    repository = ReadRepository()
    connected = asyncio.Event()
    connected.set()
    connection = SimpleNamespace(connected=connected, is_closed=False)
    channel = SimpleNamespace(is_closed=False)
    consumer = SimpleNamespace(is_running=True)
    supervisor = asyncio.create_task(asyncio.sleep(30))
    app.state.repository = repository
    app.state.rabbit = SimpleNamespace(connection=connection, channel=channel)
    app.state.consumer = consumer
    app.state.supervisor = supervisor
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            assert (await client.get("/ready")).status_code == 200
            repository.available = False
            assert (await client.get("/ready")).status_code == 503
            repository.available = True
            connected.clear()
            assert (await client.get("/ready")).status_code == 503
            connected.set()
            consumer.is_running = False
            assert (await client.get("/ready")).status_code == 503
            consumer.is_running = True
            supervisor.cancel()
            await asyncio.gather(supervisor, return_exceptions=True)
            assert (await client.get("/ready")).status_code == 503
    finally:
        supervisor.cancel()
        await asyncio.gather(supervisor, return_exceptions=True)


@pytest.mark.asyncio
async def test_stop_is_bounded_when_broker_cancel_stalls() -> None:
    async def stalled_cancel(_tag: str) -> None:
        await asyncio.Event().wait()

    rabbit = SimpleNamespace(
        queue=SimpleNamespace(cancel=stalled_cancel),
        channel=SimpleNamespace(is_closed=False),
    )
    consumer = Consumer(rabbit, ReadRepository())  # type: ignore[arg-type]
    consumer._consumer_tag = "test"  # noqa: SLF001
    await asyncio.wait_for(consumer.stop(timeout=0.05), timeout=0.5)
    assert not consumer.is_running


@pytest.mark.asyncio
async def test_stop_drains_active_work_before_returning() -> None:
    completed = asyncio.Event()

    async def cancel(_tag: str) -> None:
        return None

    async def finish_work() -> None:
        await asyncio.sleep(0.02)
        completed.set()

    rabbit = SimpleNamespace(
        queue=SimpleNamespace(cancel=cancel),
        channel=SimpleNamespace(is_closed=False),
    )
    consumer = Consumer(rabbit, ReadRepository())  # type: ignore[arg-type]
    consumer._consumer_tag = "test"  # noqa: SLF001
    task = asyncio.create_task(finish_work())
    consumer._active.add(task)  # noqa: SLF001
    await consumer.stop(timeout=0.2)
    assert completed.is_set()


def test_uvicorn_and_application_logs_are_json_without_query_secret(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="INFO", json_logs=True)
    structlog.get_logger().info("application_test", call_id="test")
    logging.getLogger("uvicorn.access").info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1",
        "GET",
        "/health?token=secret",
        "1.1",
        200,
    )
    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert records[0]["event"] == "application_test"
    assert records[0]["call_id"] == "test"
    assert records[1]["event"] == "http_access"
    assert records[1]["path"] == "/health"
    assert records[1]["status_code"] == 200
    assert all("secret" not in json.dumps(record) for record in records)
    assert all("timestamp" in record and "level" in record for record in records)


@pytest.mark.asyncio
async def test_supervisor_requests_shutdown_after_consumer_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
    consumer = SimpleNamespace(failed=failed)
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(os, "kill", lambda pid, sig: signals.append((pid, sig)))
    failed.set_result(RuntimeError("consumer exited"))
    await _supervise_consumer(consumer)  # type: ignore[arg-type]
    assert signals and signals[0][0] == os.getpid()
