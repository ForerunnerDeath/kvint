"""Structured logging configuration."""

import logging
from collections.abc import MutableMapping
from typing import Any

import structlog


def _safe_access_event(
    _logger: Any, _method: str, event: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    record = event.get("_record")
    if isinstance(record, logging.LogRecord) and record.name == "uvicorn.access":
        args = event.pop("positional_args", ())
        event["event"] = "http_access"
        if isinstance(args, tuple) and len(args) == 5:
            event["method"] = args[1]
            event["path"] = str(args[2]).split("?", 1)[0]
            event["status_code"] = args[4]
    else:
        event.pop("positional_args", None)
    return event


def configure_logging(*, level: str, json_logs: bool) -> None:
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer()
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        pass_foreign_args=True,
        processors=[
            _safe_access_event,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level.upper())
    structlog.configure(
        processors=[
            *shared,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(level.upper()),
        cache_logger_on_first_use=True,
    )
