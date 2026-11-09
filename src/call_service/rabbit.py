"""Connect to the preconfigured RabbitMQ quorum queue without redeclaring it."""

import asyncio
from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractRobustConnection

from call_service.config import Settings

CALLS_QUEUE = "calls.queue"


@dataclass
class RabbitResources:
    connection: AbstractRobustConnection
    channel: AbstractChannel
    queue: AbstractQueue

    async def close(self) -> None:
        await self.connection.close()


async def connect_consumer(
    settings: Settings, *, queue_name: str = CALLS_QUEUE
) -> RabbitResources:
    connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=5)
    try:
        channel = await connection.channel(publisher_confirms=False)
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(queue_name, passive=True)
        return RabbitResources(connection, channel, queue)
    except (Exception, asyncio.CancelledError):
        await connection.close()
        raise
