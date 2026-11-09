"""Verify the live stage-1 RabbitMQ topology through the Management API."""

import base64
import json
import os
import urllib.request
from typing import Any

from dotenv import dotenv_values


def main() -> None:
    local = dotenv_values(".env")
    user = os.getenv("RABBITMQ_USER") or local.get("RABBITMQ_USER") or "call_dev"
    password = (
        os.getenv("RABBITMQ_PASSWORD")
        or local.get("RABBITMQ_PASSWORD")
        or "call_dev"
    )
    base_url = (
        os.getenv("RABBITMQ_MANAGEMENT_URL")
        or local.get("RABBITMQ_MANAGEMENT_URL")
        or "http://127.0.0.1:15672"
    )
    token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")

    def get(path: str) -> Any:
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}/api/{path}",
            headers={"Authorization": f"Basic {token}"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)

    queue = get("queues/%2F/calls.queue")
    dlq = get("queues/%2F/calls.dlq")
    exchange = get("exchanges/%2F/calls.dlx")
    bindings = get("exchanges/%2F/calls.dlx/bindings/source")
    policy = get("policies/%2F/calls-main-quorum")

    expected = {
        "delayed-retry-type": "failed",
        "delayed-retry-min": 2000,
        "delayed-retry-max": 10000,
        "delivery-limit": 5,
        "dead-letter-exchange": "calls.dlx",
        "dead-letter-routing-key": "calls.dead",
        "dead-letter-strategy": "at-least-once",
        "overflow": "reject-publish",
    }
    assert queue["type"] == "quorum" and queue["durable"] is True
    assert queue["policy"] == "calls-main-quorum"
    assert queue["arguments"]["x-queue-type"] == "quorum"
    assert dlq["durable"] is True
    assert exchange["type"] == "direct" and exchange["durable"] is True
    assert any(
        item["destination"] == "calls.dlq"
        and item["routing_key"] == "calls.dead"
        for item in bindings
    )
    assert policy["definition"] == expected
    assert queue["effective_policy_definition"] == expected
    print("RabbitMQ topology and effective policy: OK")


if __name__ == "__main__":
    main()
