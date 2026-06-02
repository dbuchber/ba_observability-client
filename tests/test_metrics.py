"""Tests for OpenTelemetry metrics: toggle resolution, no-op behavior, instrument cache."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from observability_client import ObservabilityClient
from observability_client import client as client_module


def _otel_or_skip() -> None:
    if not client_module._OTEL_AVAILABLE:
        pytest.skip("OpenTelemetry SDK not installed.")


class _RecordCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class TestMetricsToggleResolution:
    def test_metrics_follow_tracing_by_default(self, make_client) -> None:
        # script profile: tracing off -> metrics off
        script_client = make_client(profile="script")
        assert script_client.settings.enable_metrics is False
        assert script_client.metric_meter is None

    def test_metrics_follow_enabled_tracing(self, make_client) -> None:
        _otel_or_skip()
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_auto_instrumentation=False,
        )
        assert client.settings.enable_metrics is True
        assert client.metric_meter is not None

    def test_explicit_disable_overrides_tracing(self, make_client) -> None:
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_tracing=True,
            enable_auto_instrumentation=False,
            enable_metrics=False,
        )
        assert client.settings.enable_metrics is False
        assert client.metric_meter is None

    def test_from_env_parses_enable_metrics(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OBSERVABILITY_ENABLE_TRACING", "false")
        monkeypatch.setenv("OBSERVABILITY_ENABLE_METRICS", "true")
        monkeypatch.setenv("OBSERVABILITY_ENABLE_MLFLOW", "false")
        _otel_or_skip()
        client = ObservabilityClient.from_env(service_name="svc", env="test")
        try:
            assert client.settings.enable_metrics is True
            assert client.metric_meter is not None
        finally:
            client.close()


class TestMetricsNoOpWhenDisabled:
    def test_counter_histogram_up_down_are_silent_no_ops(self, make_client) -> None:
        client = make_client(profile="script")  # metrics disabled

        # Must not raise and must not create any instrument.
        client.counter("demo.requests", 2, attributes={"route": "/a"})
        client.histogram("demo.latency_ms", 5.0, unit="ms")
        client.up_down_counter("demo.inflight", 1)

        assert client._user_instruments == {}

    def test_bound_wrapper_metrics_are_no_ops(self, make_client) -> None:
        client = make_client(profile="script")
        bound = client.bind(request_id="rid-1")

        # Delegated through the bound wrapper; still a no-op with metrics off.
        bound.counter("demo.requests", 1)
        bound.histogram("demo.latency_ms", 1.0)

        assert client._user_instruments == {}


class TestInstrumentCache:
    def test_same_name_reuses_one_instrument(self, make_client) -> None:
        _otel_or_skip()
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_auto_instrumentation=False,
        )

        client.counter("demo.requests", 1)
        client.counter("demo.requests", 1)
        client.counter("demo.requests", 1)

        assert len(client._user_instruments) == 1
        assert ("counter", "demo.requests") in client._user_instruments

    def test_different_kinds_and_names_are_distinct(self, make_client) -> None:
        _otel_or_skip()
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_auto_instrumentation=False,
        )

        client.counter("a", 1)
        client.histogram("b", 1.0)
        client.up_down_counter("c", 1)

        keys = set(client._user_instruments)
        assert keys == {("counter", "a"), ("histogram", "b"), ("up_down_counter", "c")}


class TestSelfMetricsInstalled:
    def test_self_metric_instruments_created_when_enabled(self, make_client) -> None:
        _otel_or_skip()
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_auto_instrumentation=False,
        )

        assert client._metric_loki_enqueued is not None
        assert client._metric_loki_dropped is not None
        assert client._metric_loki_failures is not None
        assert client._metric_http_server_duration is not None
        assert client._metric_http_client_duration is not None

    def test_self_metric_instruments_absent_when_disabled(self, make_client) -> None:
        client = make_client(profile="script")

        assert client._metric_loki_enqueued is None
        assert client._metric_http_client_duration is None

    def test_queue_depth_gauge_callback_reads_live_queue(self, make_client) -> None:
        _otel_or_skip()
        client = make_client(
            profile="service",
            enable_mlflow=False,
            enable_auto_instrumentation=False,
        )

        observations = client._observe_loki_queue_depth(None)
        assert len(observations) == 1
        assert observations[0].value == client._loki_queue.qsize()


class TestMetricsDependencyMissingWarning:
    def test_warns_when_metrics_requested_but_otel_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        make_client,
    ) -> None:
        monkeypatch.setattr(client_module, "_OTEL_AVAILABLE", False)

        logger = logging.getLogger("observability.metrics-warning-svc")
        capture = _RecordCapture()
        logger.addHandler(capture)

        client = make_client(
            service_name="metrics-warning-svc",
            enable_tracing=False,
            enable_metrics=True,
            enable_mlflow=False,
        )

        try:
            messages = [record.getMessage() for record in capture.records]
            assert "metrics_dependency_missing" in messages
            assert client.metric_meter is None
        finally:
            logger.removeHandler(capture)


class TestMeterProviderGuard:
    def test_existing_provider_reused_instead_of_clobbered(
        self,
        monkeypatch: pytest.MonkeyPatch,
        make_client,
    ) -> None:
        _otel_or_skip()
        from opentelemetry.sdk.metrics import MeterProvider

        existing_provider = MeterProvider()
        monkeypatch.setattr(
            client_module.metrics, "get_meter_provider", lambda: existing_provider
        )

        set_calls: list[Any] = []
        monkeypatch.setattr(
            client_module.metrics,
            "set_meter_provider",
            lambda provider: set_calls.append(provider),
        )

        logger = logging.getLogger("observability.meter-reuse-svc")
        capture = _RecordCapture()
        logger.addHandler(capture)

        try:
            client = make_client(
                service_name="meter-reuse-svc",
                profile="service",
                enable_mlflow=False,
                enable_auto_instrumentation=False,
            )

            assert set_calls == []
            assert client.metric_meter is not None
            # A reused process-global provider is not owned by this client, so
            # close() must not try to shut it down.
            assert client._meter_provider is None
            assert "meter_provider_already_installed" in [
                record.getMessage() for record in capture.records
            ]
        finally:
            logger.removeHandler(capture)
