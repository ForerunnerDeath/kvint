"""Reconnect an isolated test consumer without touching the working queue."""

import asyncio
from time import monotonic
from uuid import uuid4

import aio_pika
import httpx
import pytest
from tests.integration.test_rabbit_delivery import event_body

from call_service.config import get_settings
from call_service.consumer import Consumer
from call_service.main import create_app
from call_service.mongo import MongoRepository
from call_service.rabbit import connect_consumer


@pytest.mark.asyncio
async def test_robust_connection_restores_consumer(
    mongo_repository: MongoRepository,
) -> None:
    settings = get_settings()
    name = f"call_service_recovery_{uuid4().hex}"
    publisher = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=5)
    channel = await publisher.channel(publisher_confirms=True)
    await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum"}
    )
    resources = await connect_consumer(settings, queue_name=name)
    consumer = Consumer(resources, mongo_repository, connection_loss_limit=15)
    app = create_app()
    app.state.repository = mongo_repository
    app.state.rabbit = resources
    app.state.consumer = consumer
    supervisor = asyncio.create_task(asyncio.sleep(30))
    app.state.supervisor = supervisor
    try:
        await consumer.start()
        transport = resources.connection.transport
        assert transport is not None
        await transport.connection.close()
        deadline = monotonic() + 8
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            while resources.connection.connected.is_set():
                assert monotonic() < deadline
                await asyncio.sleep(0.05)
            assert (await client.get("/ready")).status_code == 503
        while not resources.connection.connected.is_set():
            assert monotonic() < deadline
            await asyncio.sleep(0.05)
        await channel.default_exchange.publish(
            aio_pika.Message(event_body(call_id=name)), routing_key=name
        )
        while await mongo_repository.calls.count_documents({"call_id": name}) != 1:
            assert monotonic() < deadline
            await asyncio.sleep(0.05)
        assert consumer.is_running
        assert not consumer.failed.done()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            assert (await client.get("/ready")).status_code == 200
        restored_transport = resources.connection.transport
        assert restored_transport is not None
        await restored_transport.connection.close()
        while resources.connection.connected.is_set():
            assert monotonic() < deadline
            await asyncio.sleep(0.05)
        await asyncio.wait_for(consumer.stop(timeout=0.2), timeout=1.5)
    finally:
        supervisor.cancel()
        await asyncio.gather(supervisor, return_exceptions=True)
        await consumer.stop(timeout=2)
        await resources.close()
        await channel.queue_delete(name)
        await publisher.close()
