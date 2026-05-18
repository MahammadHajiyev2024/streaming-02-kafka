"""tests/test_smoke.py - Smoke test for the example.

WHY: Professional Python projects include tests to verify that code runs
     correctly and to catch problems early when changes are made.
     Running tests is part of the standard workflow in every module.

OBS: You do not need to read or modify this file.
     It exists so that `uv run python -m pytest` passes.
"""

from typing import Any

from streaming import kafka_consumer_case, kafka_producer_case


def test_consumer_module_imports() -> None:
    """Consumer module should import without running Kafka operations."""
    assert kafka_consumer_case is not None


def test_producer_module_imports() -> None:
    """Producer module should import without running Kafka operations."""
    assert kafka_producer_case is not None


def test_consumer_process_message_returns_nonempty_row() -> None:
    """Consumer process_message() should return a nonempty row."""
    row: dict[str, Any] = {
        "order_id": "ORDER-001",
        "region_id": "US-MO",
    }

    result = kafka_consumer_case.process_message(row)

    assert isinstance(result, dict)
    assert result
    assert all(str(key).strip() for key in result)


def test_producer_get_message_key_returns_nonempty_string() -> None:
    """Producer get_message_key() should return a nonempty key."""
    message: dict[str, Any] = {
        "order_id": "ORDER-001",
        "region_id": "US-MO",
    }

    key = kafka_producer_case.get_message_key(message)

    assert isinstance(key, str)
    assert key.strip()
