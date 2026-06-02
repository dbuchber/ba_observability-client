"""Tests for Loki push delivery: body shape, queue, strict mode, overflow."""

from __future__ import annotations

import json
import time
from queue import Queue

import pytest


def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class TestPushModeRouting:
    def test_log_event_without_push_does_not_deliver(self, make_client, loki_capture) -> None:
        client = make_client()

        client.log_event("ignored")

        # Give the worker a chance to fire even though nothing was enqueued.
        time.sleep(0.05)
        assert loki_capture["bodies"] == []

    def test_log_event_push_async_delivers_body(self, make_client, loki_capture) -> None:
        client = make_client()

        client.log_event("async_event", push=True, request_id="r-1")

        assert _wait_for(lambda: len(loki_capture["bodies"]) >= 1)
        body, _endpoint = loki_capture["bodies"][0]
        payload_line = body["streams"][0]["values"][0][1]
        payload = json.loads(payload_line)
        assert payload["message"] == "async_event"
        assert payload["request_id"] == "r-1"


class TestStrictMode:
    def test_strict_mode_uses_synchronous_path(self, make_client, loki_capture) -> None:
        client = make_client()

        client.log_event("strict_event", push=True, raise_on_error=True)

        # Strict path must deliver before returning — no wait needed.
        assert len(loki_capture["bodies"]) == 1

    def test_strict_mode_raises_on_failure(self, make_client, loki_capture) -> None:
        client = make_client()
        loki_capture["fail"] = True

        with pytest.raises(RuntimeError, match="Failed to push log"):
            client.log_event("strict_event", push=True, raise_on_error=True)

    def test_async_mode_swallows_failure(self, make_client, loki_capture) -> None:
        client = make_client()
        loki_capture["fail"] = True

        # Must not raise. Worker thread silently drops the failure.
        client.log_event("async_event", push=True)
        time.sleep(0.05)


class TestLokiBody:
    def test_body_contains_payload_and_labels(self, make_client) -> None:
        client = make_client(service_name="payload-svc", env="prod")

        body = client._build_loki_body(
            message="hello",
            level="info",
            fields={"request_id": "r-1"},
        )

        stream = body["streams"][0]["stream"]
        assert stream["service"] == "payload-svc"
        assert stream["env"] == "prod"
        assert stream["level"] == "info"
        assert stream["job"] == "host-python"

        timestamp, payload_line = body["streams"][0]["values"][0]
        assert timestamp.isdigit()
        payload = json.loads(payload_line)
        assert payload["message"] == "hello"
        assert payload["level"] == "info"
        assert payload["request_id"] == "r-1"

    def test_high_cardinality_fields_kept_out_of_labels(self, make_client) -> None:
        client = make_client()

        body = client._build_loki_body(
            message="event",
            level="info",
            fields={"request_id": "high-card-1", "query_hash": "abc123"},
        )

        labels = body["streams"][0]["stream"]
        assert "request_id" not in labels
        assert "query_hash" not in labels

        payload = json.loads(body["streams"][0]["values"][0][1])
        assert payload["request_id"] == "high-card-1"
        assert payload["query_hash"] == "abc123"


class TestSparqlEvent:
    def test_log_sparql_emits_normalized_event(self, make_client, loki_capture) -> None:
        client = make_client()

        client.log_sparql(
            query_hash="abc",
            stage="retrieve",
            latency_ms=12.3,
            result_count=5,
            sparql_endpoint="http://fuseki/ds/query",
        )

        # log_sparql goes through log_event without push=True, so nothing pushed.
        time.sleep(0.05)
        assert loki_capture["bodies"] == []


class TestQueueOverflow:
    def test_overflow_increments_drop_counter(self, make_client, loki_capture) -> None:
        client = make_client()
        client._stop_loki_worker()
        client._loki_queue = Queue(maxsize=2)

        for _ in range(5):
            client._enqueue_loki_event(message="m", level="info", fields={})

        assert client._loki_dropped_events_count == 3

    def test_overflow_emits_throttled_warning_event(self, make_client, loki_capture) -> None:
        import logging

        class _RecordCapture(logging.Handler):
            def __init__(self) -> None:
                super().__init__(level=logging.DEBUG)
                self.records: list[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        client = make_client()
        client._stop_loki_worker()
        client._loki_queue = Queue(maxsize=1)
        # Ensure the warning interval has elapsed so the first overflow warns.
        client._loki_last_drop_warning_monotonic = 0.0

        cap = _RecordCapture()
        client._logger.addHandler(cap)

        for _ in range(5):
            client._enqueue_loki_event(message="m", level="info", fields={})

        messages = [record.getMessage() for record in cap.records]
        assert "loki_queue_full_dropping_events" in messages


class TestAsyncWorker:
    def test_worker_drains_multiple_events(self, make_client, loki_capture) -> None:
        client = make_client()

        for index in range(5):
            client.log_event(f"event-{index}", push=True)

        assert _wait_for(lambda: len(loki_capture["bodies"]) >= 5)

    def test_worker_continues_after_failure(self, make_client, loki_capture) -> None:
        client = make_client()

        loki_capture["fail"] = True
        client.log_event("will-fail", push=True)
        time.sleep(0.05)
        loki_capture["fail"] = False

        client.log_event("will-succeed", push=True)

        assert _wait_for(lambda: len(loki_capture["bodies"]) >= 1)
        payload = json.loads(loki_capture["bodies"][0][0]["streams"][0]["values"][0][1])
        assert payload["message"] == "will-succeed"


class TestPostLokiBodyUrlErrors:
    """Real ``_post_loki_body`` path (no mock): malformed URLs must be caught.

    ``request.Request()`` parses the URL in its constructor and raises
    ``ValueError`` for an unknown scheme (e.g. ``"not-a-url"``) before any
    network call. These tests exercise the unmocked method to guard against
    that ``ValueError`` escaping the ``try`` block.
    """

    @pytest.mark.parametrize("endpoint", ["not-a-url", "unknownscheme://host/push"])
    def test_async_mode_returns_false_for_bad_url(self, make_client, endpoint) -> None:
        client = make_client(loki_push_endpoint=endpoint)

        # Must return False, not raise, even when the URL is unparsable.
        result = client._push_log_sync(
            message="m", level="info", fields={}, raise_on_error=False
        )
        assert result is False

    def test_strict_mode_wraps_bad_url_in_runtime_error(self, make_client) -> None:
        client = make_client(loki_push_endpoint="not-a-url")

        with pytest.raises(RuntimeError, match="Failed to push log"):
            client._push_log_sync(
                message="m", level="info", fields={}, raise_on_error=True
            )


class TestDeliveryFailureCounter:
    def test_failure_count_increments_per_failed_delivery(
        self, make_client, loki_capture
    ) -> None:
        client = make_client()
        loki_capture["fail"] = True

        for _ in range(3):
            client.log_event("fails", push=True)

        assert _wait_for(lambda: client._loki_delivery_failures_count >= 3)
        assert client._loki_delivery_failures_count == 3

    def test_failure_count_unchanged_when_delivery_succeeds(
        self, make_client, loki_capture
    ) -> None:
        client = make_client()

        for _ in range(3):
            client.log_event("succeeds", push=True)

        assert _wait_for(lambda: len(loki_capture["bodies"]) >= 3)
        assert client._loki_delivery_failures_count == 0

    def test_failure_emits_throttled_warning(self, make_client, loki_capture) -> None:
        import logging

        class _RecordCapture(logging.Handler):
            def __init__(self) -> None:
                super().__init__(level=logging.DEBUG)
                self.records: list[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        client = make_client()
        cap = _RecordCapture()
        client._logger.addHandler(cap)
        loki_capture["fail"] = True

        client.log_event("fails", push=True)

        def _saw_warning() -> bool:
            return "loki_delivery_failed" in [r.getMessage() for r in cap.records]

        assert _wait_for(_saw_warning)
