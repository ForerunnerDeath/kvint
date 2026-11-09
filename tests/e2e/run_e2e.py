"""Isolated Windows-compatible Docker Compose end-to-end acceptance run."""

import argparse
import asyncio
import os
import secrets
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import aio_pika
import httpx
from aio_pika import DeliveryMode, Message
from pymongo import MongoClient
from pymongo.database import Database

from call_service.schemas import CallEvent

ROOT = Path(__file__).resolve().parents[2]


def command(args: list[str], env: dict[str, str], timeout: int = 180) -> str:
    result = subprocess.run(
        args,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise AssertionError(
            f"Command failed ({result.returncode}): {' '.join(args)}\n"
            f"{result.stdout[-1500:]}\n{result.stderr[-1500:]}"
        )
    return result.stdout


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(label: str, predicate: Callable[[], bool], timeout: float = 60) -> None:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        try:
            if predicate():
                print(f"OK: {label}", flush=True)
                return
        except (httpx.HTTPError, OSError):
            pass
        sleep(0.25)
    raise AssertionError(f"Timed out: {label}")


def sample_event(campaign_id: str, call_id: str, status: str = "done") -> bytes:
    return (
        CallEvent.model_validate(
            {
                "schema_version": 1,
                "campaign": {
                    "campaign_id": campaign_id,
                    "name": f"E2E {campaign_id}",
                    "created_at": "2026-10-20T10:00:00Z",
                },
                "call": {
                    "call_id": call_id,
                    "campaign_id": campaign_id,
                    "phone": "79000000000",
                    "status": status,
                    "created_at": "2026-10-20T10:01:00Z",
                },
            }
        )
        .model_dump_json()
        .encode()
    )


async def publish(url: str, body: bytes) -> None:
    connection = await aio_pika.connect_robust(url, timeout=5)
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        result = await channel.default_exchange.publish(
            Message(
                body,
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=uuid4().hex,
            ),
            routing_key="calls.queue",
            mandatory=True,
            timeout=5,
        )
        assert result is not None
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--smoke", action="store_true")
    modes.add_argument("--integration", action="store_true")
    args = parser.parse_args()
    project = f"kvint_e2e_{uuid4().hex[:10]}"
    assert project.startswith("kvint_e2e_")
    ports = [free_port() for _ in range(4)]
    assert len(set(ports)) == 4
    api_port, amqp_port, management_port, mongo_port = ports
    password = secrets.token_urlsafe(24)
    env = os.environ.copy()
    env.update(
        {
            "API_PORT": str(api_port),
            "AMQP_PORT": str(amqp_port),
            "RABBITMQ_MANAGEMENT_PORT": str(management_port),
            "MONGO_TEST_PORT": str(mongo_port),
            "RABBITMQ_USER": "call_e2e",
            "RABBITMQ_PASSWORD": password,
            "RABBITMQ_HOST": "127.0.0.1",
            "RABBITMQ_PORT": str(amqp_port),
            "RABBITMQ_MANAGEMENT_URL": f"http://127.0.0.1:{management_port}",
            "MONGO_URL": f"mongodb://127.0.0.1:{mongo_port}",
            "TEST_MONGO_URL": f"mongodb://127.0.0.1:{mongo_port}",
            "MONGO_DATABASE": "calls",
            "E2E_APP_IMAGE": f"{project}:e2e",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUTF8": "1",
        }
    )
    compose = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        "docker-compose.yml",
        "--file",
        "tests/e2e/docker-compose.yml",
    ]
    volume_filter = f"label=com.docker.compose.project={project}"

    def volumes() -> set[str]:
        output = command(
            ["docker", "volume", "ls", "--quiet", "--filter", volume_filter],
            env,
        )
        return set(output.splitlines())

    assert not volumes(), "E2E project name already has volumes"
    assert not command([*compose, "ps", "--quiet"], env).strip()
    expected_volumes = {f"{project}_mongo_data", f"{project}_rabbitmq_data"}
    started = False
    mongo_client: MongoClient[dict[str, Any]] | None = None
    mongo: Database[dict[str, Any]] | None = None
    api = httpx.Client(base_url=f"http://127.0.0.1:{api_port}", timeout=3)
    management = httpx.Client(
        base_url=f"http://127.0.0.1:{management_port}",
        auth=("call_e2e", password),
        timeout=3,
    )
    amqp_url = f"amqp://call_e2e:{quote(password, safe='')}@127.0.0.1:{amqp_port}/"

    def compose_command(*args: str, timeout: int = 180) -> str:
        return command([*compose, *args], env, timeout)

    def ready_status() -> int | None:
        try:
            return api.get("/ready").status_code
        except httpx.HTTPError:
            return None

    def queue(name: str) -> dict[str, Any]:
        response = management.get(f"/api/queues/%2F/{name}")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("RabbitMQ queue response must be a JSON object")
        return data

    def call_count(call_id: str) -> int:
        assert mongo is not None
        return mongo.calls.count_documents({"call_id": call_id})

    def restart_count() -> int:
        container = compose_command("ps", "--quiet", "app").strip()
        return int(
            command(
                ["docker", "inspect", "--format", "{{.RestartCount}}", container],
                env,
            ).strip()
        )

    try:
        print(f"E2E project: {project}; ports: {ports}", flush=True)
        started = True
        compose_command("up", "--build", "--detach", timeout=300)
        assert volumes() == expected_volumes
        wait_for("all services ready", lambda: ready_status() == 200, 120)
        mongo_client = MongoClient[dict[str, Any]](
            env["MONGO_URL"], serverSelectionTimeoutMS=3000
        )
        mongo = mongo_client["calls"]
        assert mongo.campaigns.count_documents({}) == 0
        assert mongo.calls.count_documents({}) == 0
        print("OK: fresh MongoDB collections empty", flush=True)
        def topology_applied() -> bool:
            try:
                command([sys.executable, "scripts/verify_rabbitmq.py"], env)
            except AssertionError:
                return False
            return True

        wait_for("RabbitMQ topology and effective policy", topology_applied, 60)

        if args.integration:
            output = command(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/integration/test_rabbit_recovery.py",
                    "tests/integration/test_rabbit_delivery.py",
                    "tests/integration/test_mongo_handler.py",
                    "tests/integration/test_api_read.py",
                    "-p",
                    "no:cacheprovider",
                    "-q",
                ],
                env,
                timeout=180,
            )
            safe_output = output.encode("ascii", "backslashreplace").decode("ascii")
            print(safe_output, flush=True)
            for attempt in range(3):
                output = command(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        "tests/integration/test_rabbit_recovery.py",
                        "-p",
                        "no:cacheprovider",
                        "-q",
                    ],
                    env,
                    timeout=30,
                )
                safe_output = output.strip().encode("ascii", "backslashreplace")
                print(
                    f"Recovery repeat {attempt + 1}: {safe_output.decode('ascii')}",
                    flush=True,
                )
            return

        assert api.get("/health").status_code == 200
        command([sys.executable, "producer.py"], env)
        wait_for(
            "two campaigns and four calls",
            lambda: (
                mongo is not None
                and mongo.campaigns.count_documents({}) == 2
                and mongo.calls.count_documents({}) == 4
            ),
        )
        for campaign_id in ("camp_001", "camp_002"):
            response = api.get(f"/campaigns/{campaign_id}/calls")
            assert response.status_code == 200
            calls = response.json()
            assert len(calls) == 2
            assert all(
                call["campaign_id"] == campaign_id and "_id" not in call
                for call in calls
            )
        command([sys.executable, "producer.py"], env)
        wait_for(
            "four duplicate events processed",
            lambda: compose_command("logs", "--no-color", "app").count(
                '"event": "duplicate_ignored"'
            )
            >= 4,
        )
        wait_for(
            "duplicate deliveries acknowledged",
            lambda: int(queue("calls.queue")["messages"]) == 0,
        )
        assert mongo.campaigns.count_documents({}) == 2
        assert mongo.calls.count_documents({}) == 4
        print("OK: producer repeat kept 2 campaigns and 4 calls", flush=True)
        if args.smoke:
            print("E2E smoke PASS", flush=True)
            return

        missing = api.get("/campaigns/absent/calls")
        assert missing.status_code == 200
        assert missing.headers["content-type"].startswith("text/plain")
        assert missing.text == "Кампания не найдена"
        mongo.campaigns.insert_one(
            {
                "campaign_id": "e2e_empty",
                "name": "Empty",
                "created_at": "2026-10-20T10:00:00Z",
            }
        )
        assert api.get("/campaigns/e2e_empty/calls").json() == []
        docs = api.get("/docs")
        assert docs.status_code == 200
        openapi = api.get("/openapi.json").json()
        content = openapi["paths"]["/campaigns/{campaign_id}/calls"]["get"][
            "responses"
        ]["200"]["content"]
        assert {"application/json", "text/plain"} <= content.keys()
        print("OK: API, missing/empty campaign and OpenAPI", flush=True)

        dlq_before = int(queue("calls.dlq")["messages_ready"])
        assert dlq_before == 0
        asyncio.run(publish(amqp_url, b"{invalid"))
        wait_for(
            "invalid JSON in DLQ",
            lambda: int(queue("calls.dlq")["messages_ready"]) == 1,
        )
        asyncio.run(publish(amqp_url, b'{"schema_version":2}'))
        wait_for(
            "invalid Pydantic event in DLQ",
            lambda: int(queue("calls.dlq")["messages_ready"]) == 2,
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_good_1")))
        wait_for("dedicated valid event saved", lambda: call_count("e2e_good_1") == 1)
        asyncio.run(
            publish(amqp_url, sample_event("e2e_fault", "e2e_good_1", "conflict"))
        )
        wait_for(
            "conflicting call in DLQ",
            lambda: int(queue("calls.dlq")["messages_ready"]) == 3,
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_good_2")))
        wait_for(
            "consumer continues after rejects", lambda: call_count("e2e_good_2") == 1
        )
        assert call_count("e2e_good_1") == 1
        response = management.post(
            "/api/queues/%2F/calls.dlq/get",
            json={"count": 3, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        response.raise_for_status()
        dead = response.json()
        assert len(dead) == 3
        assert all(
            item["properties"]["headers"]["x-death"][0]["reason"] == "rejected"
            for item in dead
        )
        print(
            "OK: invalid JSON, schema, conflict -> DLQ; valid event continued",
            flush=True,
        )

        command(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/integration/test_rabbit_delivery.py",
                "-p",
                "no:cacheprovider",
                "-q",
            ],
            env,
            timeout=120,
        )
        print(
            "OK: real broker delayed retry, delivery_limit reason and ACK tests",
            flush=True,
        )

        compose_command("stop", "rabbitmq")
        wait_for(
            "readiness 503 during broker outage", lambda: ready_status() == 503, 15
        )
        compose_command("start", "rabbitmq")
        wait_for(
            "readiness restored after broker restart", lambda: ready_status() == 200, 90
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_after_rabbit")))
        wait_for(
            "post-RabbitMQ recovery call saved",
            lambda: call_count("e2e_after_rabbit") == 1,
        )

        wait_for(
            "RabbitMQ DLQ statistics restored",
            lambda: "messages_ready" in queue("calls.dlq"),
            30,
        )
        dlq_at_mongo_outage = int(queue("calls.dlq")["messages_ready"])
        compose_command("stop", "mongo")
        wait_for(
            "readiness 503 during MongoDB outage", lambda: ready_status() == 503, 20
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_after_mongo")))
        wait_for(
            "message held unacked during MongoDB outage",
            lambda: int(queue("calls.queue")["messages_unacknowledged"]) >= 1,
            20,
        )
        assert int(queue("calls.dlq")["messages_ready"]) == dlq_at_mongo_outage
        compose_command("start", "mongo")
        wait_for(
            "readiness restored after MongoDB restart",
            lambda: ready_status() == 200,
            90,
        )
        wait_for(
            "held call saved after MongoDB recovery",
            lambda: call_count("e2e_after_mongo") == 1,
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_after_mongo")))
        wait_for(
            "post-outage duplicate acknowledged",
            lambda: int(queue("calls.queue")["messages"]) == 0,
        )
        assert call_count("e2e_after_mongo") == 1
        assert int(queue("calls.dlq")["messages_ready"]) == dlq_at_mongo_outage
        print(
            "OK: real MongoDB outage held delivery; recovery created one call",
            flush=True,
        )

        before = restart_count()
        compose_command("stop", "rabbitmq")
        wait_for(
            "readiness 503 before supervisor restart", lambda: ready_status() == 503, 15
        )
        wait_for(
            "supervisor triggered app restart", lambda: restart_count() > before, 65
        )
        compose_command("start", "rabbitmq")
        wait_for(
            "app ready after supervised restart", lambda: ready_status() == 200, 90
        )
        asyncio.run(publish(amqp_url, sample_event("e2e_fault", "e2e_after_restart")))
        wait_for(
            "consumer processes after app restart",
            lambda: call_count("e2e_after_restart") == 1,
        )
        print(
            "E2E PASS: isolated Compose, API, DLQ, retry, outages, supervisor",
            flush=True,
        )
    finally:
        if mongo_client is not None:
            mongo_client.close()
        api.close()
        management.close()
        if started:
            owned = volumes()
            if owned <= expected_volumes:
                compose_command("down", "--volumes", "--remove-orphans", timeout=120)
                print(f"Removed only E2E project volumes: {sorted(owned)}", flush=True)
                image_check = subprocess.run(
                    ["docker", "image", "inspect", env["E2E_APP_IMAGE"]],
                    cwd=ROOT,
                    env=env,
                    capture_output=True,
                    check=False,
                )
                if image_check.returncode == 0:
                    command(["docker", "image", "rm", env["E2E_APP_IMAGE"]], env)
            elif owned:
                print(
                    "E2E volumes retained because ownership check failed: "
                    f"{sorted(owned)}",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    main()
