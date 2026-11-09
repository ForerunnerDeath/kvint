"""RabbitMQ delivery handling, retry classification, and shutdown."""

import asyncio
import json
import traceback
from time import monotonic

import structlog
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError
from pymongo.errors import AutoReconnect, ExecutionTimeout, PyMongoError, WTimeoutError

from call_service.exceptions import PermanentEventError
from call_service.handler import handle_event
from call_service.mongo import MongoRepository
from call_service.rabbit import RabbitResources
from call_service.schemas import CallEvent

TRANSIENT_MONGO_ERRORS = (AutoReconnect, ExecutionTimeout, WTimeoutError)


class Consumer:
    def __init__(
        self,
        rabbit: RabbitResources,
        repository: MongoRepository,
        *,
        recovery_interval: float = 2.0,
        connection_loss_limit: float = 30.0,
    ) -> None:
        self.rabbit = rabbit
        self.repository = repository
        self.recovery_interval = recovery_interval
        self.connection_loss_limit = connection_loss_limit
        self.failed: asyncio.Future[BaseException] = (
            asyncio.get_running_loop().create_future()
        )
        self._stopping = asyncio.Event()
        self._active: set[asyncio.Task[object]] = set()
        self._consumer_tag: str | None = None
        self._connection_watch: asyncio.Task[None] | None = None
        self.log = structlog.get_logger().bind(component="consumer")

    async def start(self) -> None:
        self._consumer_tag = await self.rabbit.queue.consume(
            self._on_message, no_ack=False
        )
        self._connection_watch = asyncio.create_task(self._watch_connection())
        self.log.info("consumer_started")

    @property
    def is_running(self) -> bool:
        return (
            self._consumer_tag is not None
            and not self._stopping.is_set()
            and not self.failed.done()
            and self._connection_watch is not None
            and not self._connection_watch.done()
        )

    async def stop(self, timeout: float = 15.0) -> None:
        self._stopping.set()
        if self._consumer_tag is not None and not self.rabbit.channel.is_closed:
            try:
                await asyncio.wait_for(
                    self.rabbit.queue.cancel(self._consumer_tag), timeout=timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(
                    "consumer_cancel_failed", error_type=type(exc).__name__
                )
        self._consumer_tag = None
        if self._active:
            _, pending = await asyncio.wait(self._active.copy(), timeout=timeout)
            if pending:
                self.log.warning("consumer_drain_timed_out")
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=1)
        if self._connection_watch is not None:
            self._connection_watch.cancel()
            await asyncio.gather(self._connection_watch, return_exceptions=True)
        self.log.info("consumer_stopped")

    async def _watch_connection(self) -> None:
        try:
            lost_at: float | None = None
            while not self._stopping.is_set():
                connected = self.rabbit.connection.connected.is_set()
                healthy = connected and not self.rabbit.channel.is_closed
                if not healthy and lost_at is None:
                    lost_at = monotonic()
                    self.log.warning("rabbit_connection_lost")
                elif healthy and lost_at is not None:
                    lost_at = None
                    self.log.info("rabbit_connection_restored")
                if self.rabbit.connection.is_closed or (
                    lost_at is not None
                    and monotonic() - lost_at >= self.connection_loss_limit
                ):
                    self._fail(RuntimeError("RabbitMQ consumer connection unavailable"))
                    return
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._active.add(task)
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            message_id=message.message_id,
            delivery_count=(message.headers or {}).get("x-delivery-count", 0),
        )
        try:
            await self._process(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)
        finally:
            structlog.contextvars.clear_contextvars()
            if task is not None:
                self._active.discard(task)

    async def _process(self, message: AbstractIncomingMessage) -> None:
        try:
            payload = json.loads(message.body)
            event = CallEvent.model_validate(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
            self.log.warning("message_invalid", error_type=type(exc).__name__)
            await message.reject(requeue=False)
            self.log.info("message_rejected", reason="invalid")
            return

        structlog.contextvars.bind_contextvars(
            call_id=event.call.call_id, campaign_id=event.campaign.campaign_id
        )
        while not self._stopping.is_set():
            try:
                created = await handle_event(event, self.repository)
            except PermanentEventError as exc:
                self.log.warning("message_conflict", error_type=type(exc).__name__)
                await message.reject(requeue=False)
                self.log.info("message_rejected", reason="conflict")
                return
            except TRANSIENT_MONGO_ERRORS as exc:
                if await self._mongo_available():
                    await message.reject(requeue=True)
                    self.log.warning(
                        "db_retry_scheduled", error_type=type(exc).__name__
                    )
                    return
                self.log.error("mongo_unavailable", error_type=type(exc).__name__)
                if not await self._wait_for_mongo():
                    return  # Channel close will requeue this unacked delivery.
                continue  # Idempotent upserts handle an ambiguous prior write.
            except PyMongoError:
                raise  # Authorization, configuration, or other unexpected DB failure.
            else:
                await message.ack()
                self.log.info("call_saved" if created else "duplicate_ignored")
                return

    async def _mongo_available(self) -> bool:
        try:
            await self.repository.ping()
            return True
        except TRANSIENT_MONGO_ERRORS:
            return False

    async def _wait_for_mongo(self) -> bool:
        while not self._stopping.is_set():
            await asyncio.sleep(self.recovery_interval)
            if await self._mongo_available():
                self.log.info("mongo_connection_restored")
                return True
        return False

    def _fail(self, exc: BaseException) -> None:
        self.log.critical(
            "consumer_failed",
            error_type=type(exc).__name__,
            traceback="".join(traceback.format_tb(exc.__traceback__)),
        )
        if not self.failed.done():
            self.failed.set_result(exc)
