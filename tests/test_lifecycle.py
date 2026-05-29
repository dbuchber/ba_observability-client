"""Tests for client lifecycle: close(), worker shutdown, MLflow-disabled paths."""

from __future__ import annotations

import pytest


class TestClose:
    def test_close_stops_worker_thread(self, make_client) -> None:
        client = make_client()
        worker = client._loki_worker_thread

        assert worker is not None
        assert worker.is_alive()

        client.close()

        worker.join(timeout=2.0)
        assert not worker.is_alive()

    def test_close_drains_queued_events(self, make_client, loki_capture) -> None:
        client = make_client()

        for index in range(5):
            client.log_event(f"event-{index}", push=True)

        client.close()

        # After close, the worker drained the queue before exiting.
        assert len(loki_capture["bodies"]) == 5

    def test_close_without_mlflow_does_not_error(self, make_client) -> None:
        client = make_client(profile="script")

        client.close()  # must not raise even though MLflow is disabled

    def test_double_close_does_not_raise(self, make_client) -> None:
        client = make_client()

        client.close()
        client.close()  # idempotent in practice — stop event already set, no run open

    def test_double_close_only_runs_shutdown_once(self, make_client) -> None:
        client = make_client()
        calls: list[int] = []
        original_stop = client._stop_loki_worker

        def spy_stop() -> None:
            calls.append(1)
            original_stop()

        client._stop_loki_worker = spy_stop  # type: ignore[method-assign]

        client.close()
        client.close()
        client.close()

        assert len(calls) == 1

    def test_close_sets_closed_flag(self, make_client) -> None:
        client = make_client()
        assert client._closed is False

        client.close()
        assert client._closed is True


class TestPostCloseDelivery:
    def test_push_after_close_uses_synchronous_delivery(self, make_client, loki_capture) -> None:
        import json as _json

        client = make_client()
        client.close()

        # Worker is stopped; relying on async enqueue would silently drop this.
        client.log_event("late-event", push=True, source="shutdown")

        assert len(loki_capture["bodies"]) == 1
        payload = _json.loads(loki_capture["bodies"][0][0]["streams"][0]["values"][0][1])
        assert payload["message"] == "late-event"
        assert payload["source"] == "shutdown"

    def test_push_after_close_strict_still_raises(self, make_client, loki_capture) -> None:
        client = make_client()
        client.close()
        loki_capture["fail"] = True

        with pytest.raises(RuntimeError, match="Failed to push log"):
            client.log_event("late-event", push=True, raise_on_error=True)


class TestMLflowDisabled:
    def test_start_run_raises_when_mlflow_disabled(self, make_client) -> None:
        client = make_client(profile="script")

        with pytest.raises(RuntimeError, match="MLflow is disabled"):
            client.start_run()

    def test_log_metrics_is_silent_no_op_when_disabled(self, make_client) -> None:
        client = make_client(profile="script")

        # Documented behavior: silent no-op rather than raising.
        client.log_metrics({"x": 1.0})

    def test_log_params_is_silent_no_op_when_disabled(self, make_client) -> None:
        client = make_client(profile="script")

        client.log_params({"k": "v"})

    def test_log_artifact_is_silent_no_op_when_disabled(self, make_client) -> None:
        client = make_client(profile="script")

        client.log_artifact("/nonexistent/path/that/should/not/be/read.txt")


class TestWorkerStartupIdempotency:
    def test_start_worker_twice_does_not_spawn_second_thread(self, make_client) -> None:
        client = make_client()
        first = client._loki_worker_thread

        client._start_loki_worker()
        second = client._loki_worker_thread

        assert first is second
        assert first is not None
        assert first.is_alive()
