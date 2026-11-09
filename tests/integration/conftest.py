"""Real MongoDB fixture, always using a fresh isolated test database."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest_asyncio
from pymongo import AsyncMongoClient

from call_service.mongo import MongoRepository


@pytest_asyncio.fixture
async def mongo_repository() -> AsyncIterator[MongoRepository]:
    url = os.getenv("TEST_MONGO_URL", "mongodb://127.0.0.1:27018")
    name = f"call_service_test_{uuid4().hex}"
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        url, tz_aware=True, serverSelectionTimeoutMS=5000
    )
    connected = False
    try:
        await client.admin.command("ping")
        connected = True
        repository = MongoRepository(client[name])
        await repository.create_indexes()
        yield repository
    finally:
        if connected:
            if not name.startswith("call_service_test_"):
                raise RuntimeError("Refusing to remove a non-test database")
            await client.drop_database(name)
        await client.close()
