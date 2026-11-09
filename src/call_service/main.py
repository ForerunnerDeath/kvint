"""FastAPI application entry point."""

import asyncio
import os
import signal
import traceback
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from pymongo.errors import AutoReconnect

from call_service.api import router
from call_service.config import Settings, get_settings
from call_service.consumer import Consumer
from call_service.log_config import configure_logging
from call_service.mongo import MongoRepository
from call_service.rabbit import RabbitResources, connect_consumer


async def _connect_mongo(settings: Settings) -> AsyncMongoClient[dict[str, Any]]:
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        settings.mongo_url, tz_aware=True, serverSelectionTimeoutMS=2000
    )
    try:
        for attempt in range(3):
            try:
                await client.admin.command("ping")
                return client
            except AutoReconnect:
                if attempt == 2:
                    raise
                await asyncio.sleep(2)
    except BaseException:
        await client.close()
        raise
    raise RuntimeError("MongoDB startup retries exhausted")


async def _connect_rabbit(settings: Settings) -> RabbitResources:
    for attempt in range(3):
        try:
            return await connect_consumer(settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ startup retries exhausted")


async def _supervise_consumer(consumer: Consumer) -> None:
    await consumer.failed
    # A failed callback leaves its delivery unacked. Let Uvicorn shut down and
    # the container restart rather than serving indefinitely without a consumer.
    os.kill(os.getpid(), signal.SIGTERM)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    log = structlog.get_logger()
    settings = get_settings()
    mongo: AsyncMongoClient[dict[str, Any]] | None = None
    rabbit: RabbitResources | None = None
    consumer: Consumer | None = None
    supervisor: asyncio.Task[None] | None = None
    try:
        mongo = await _connect_mongo(settings)
        repository = MongoRepository(mongo[settings.mongo_database])
        await repository.create_indexes()
        rabbit = await _connect_rabbit(settings)
        consumer = Consumer(rabbit, repository)
        await consumer.start()
        supervisor = asyncio.create_task(_supervise_consumer(consumer))
        app.state.repository = repository
        app.state.rabbit = rabbit
        app.state.consumer = consumer
        app.state.supervisor = supervisor
        log.info("application_started")
        yield
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.critical(
            "application_failed",
            error_type=type(exc).__name__,
            traceback="".join(traceback.format_tb(exc.__traceback__)),
        )
        raise
    finally:
        app.state.repository = None
        app.state.consumer = None
        app.state.rabbit = None
        app.state.supervisor = None
        if consumer is not None:
            await consumer.stop()
        if supervisor is not None:
            supervisor.cancel()
            await asyncio.gather(supervisor, return_exceptions=True)
        if rabbit is not None:
            try:
                await asyncio.wait_for(rabbit.close(), timeout=5)
            except TimeoutError:
                log.warning("rabbit_close_timed_out")
        if mongo is not None:
            await mongo.close()
        log.info("application_stopped")


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(level=settings.log_level, json_logs=settings.log_json)
    app = FastAPI(title="Call Integration Service", lifespan=lifespan)
    app.include_router(router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request) -> JSONResponse:
        repository: MongoRepository | None = getattr(
            request.app.state, "repository", None
        )
        rabbit: RabbitResources | None = getattr(request.app.state, "rabbit", None)
        consumer: Consumer | None = getattr(request.app.state, "consumer", None)
        supervisor: asyncio.Task[None] | None = getattr(
            request.app.state, "supervisor", None
        )
        if (
            repository is None
            or rabbit is None
            or consumer is None
            or supervisor is None
            or supervisor.done()
            or not consumer.is_running
            or not rabbit.connection.connected.is_set()
            or rabbit.connection.is_closed
            or rabbit.channel.is_closed
        ):
            return JSONResponse({"status": "unavailable"}, status_code=503)
        try:
            await asyncio.wait_for(repository.ping(), timeout=2)
        except asyncio.CancelledError:
            raise
        except Exception:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ready"})

    return app


app = create_app()
