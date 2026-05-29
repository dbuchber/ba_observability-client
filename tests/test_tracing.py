"""Tests for tracing helpers: span fallback, request_id resolution, header normalization."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pytest

from observability_client import ObservabilityClient
from observability_client import client as client_module


@dataclass
class _FakeURL:
    path: str = "/handler"


@dataclass
class _FakeRequest:
    headers: dict[str, str]
    method: str = "GET"
    url: _FakeURL = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.url is None:
            self.url = _FakeURL()


class TestSpanFallback:
    def test_span_yields_none_when_tracing_disabled(self, make_client) -> None:
        client = make_client(enable_tracing=False)

        with client.span("noop") as active_span:
            assert active_span is None

    def test_span_yields_none_for_bound_wrapper_when_disabled(self, make_client) -> None:
        client = make_client(enable_tracing=False)

        with client.bind(stage="x").span("noop") as active_span:
            assert active_span is None


class TestResolveRequestId:
    def test_explicit_request_id_wins(self) -> None:
        resolved = ObservabilityClient._resolve_request_id(
            headers={"x-request-id": "from-header"},
            request_id="from-arg",
            request_id_header="x-request-id",
        )
        assert resolved == "from-arg"

    def test_header_used_when_no_explicit_arg(self) -> None:
        resolved = ObservabilityClient._resolve_request_id(
            headers={"x-request-id": "from-header"},
            request_id=None,
            request_id_header="x-request-id",
        )
        assert resolved == "from-header"

    def test_generated_uuid_when_neither_present(self) -> None:
        resolved = ObservabilityClient._resolve_request_id(
            headers={},
            request_id=None,
            request_id_header="x-request-id",
        )
        # 32-hex-char uuid4 .hex
        assert len(resolved) == 32
        int(resolved, 16)  # parseable as hex

    def test_blank_explicit_id_falls_back_to_header(self) -> None:
        resolved = ObservabilityClient._resolve_request_id(
            headers={"x-request-id": "from-header"},
            request_id="   ",
            request_id_header="x-request-id",
        )
        assert resolved == "from-header"

    def test_blank_header_falls_back_to_uuid(self) -> None:
        resolved = ObservabilityClient._resolve_request_id(
            headers={"x-request-id": "   "},
            request_id=None,
            request_id_header="x-request-id",
        )
        assert len(resolved) == 32


class TestNormalizeCarrierHeaders:
    def test_lowercases_and_strips_keys(self) -> None:
        normalized = ObservabilityClient._normalize_carrier_headers(
            {"X-Request-ID": "abc", "  Authorization  ": "token"}
        )
        assert normalized["x-request-id"] == "abc"
        assert normalized["authorization"] == "token"

    def test_returns_empty_for_unsupported_carrier(self) -> None:
        assert ObservabilityClient._normalize_carrier_headers(None) == {}
        assert ObservabilityClient._normalize_carrier_headers(42) == {}


class TestFastapiRequestSpan:
    def test_yields_resolved_request_id_when_tracing_disabled(self, make_client) -> None:
        client = make_client(enable_tracing=False)
        request = _FakeRequest(headers={"x-request-id": "from-fastapi"})

        with client.fastapi_request_span(request=request) as request_id:
            assert request_id == "from-fastapi"

    def test_generates_request_id_when_none_supplied(self, make_client) -> None:
        client = make_client(enable_tracing=False)
        request = _FakeRequest(headers={})

        with client.fastapi_request_span(request=request) as request_id:
            assert len(request_id) == 32

    def test_request_id_bound_into_log_context(self, make_client) -> None:
        class _RecordCapture(logging.Handler):
            def __init__(self) -> None:
                super().__init__(level=logging.DEBUG)
                self.records: list[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        client = make_client(enable_tracing=False)
        capture = _RecordCapture()
        client._logger.addHandler(capture)

        request = _FakeRequest(headers={"x-request-id": "rid-1"})
        with client.fastapi_request_span(request=request):
            client.log_event("inside_request")

        client.log_event("outside_request")

        inside_ctx: dict[str, Any] = capture.records[0].ctx  # type: ignore[attr-defined]
        outside_ctx: dict[str, Any] = capture.records[1].ctx  # type: ignore[attr-defined]
        assert inside_ctx.get("request_id") == "rid-1"
        assert "request_id" not in outside_ctx


class TestHttpxRequestForwardsRequestId:
    def test_bound_request_id_added_as_header(self, make_client) -> None:
        client = make_client(enable_tracing=False)
        observed: dict[str, Any] = {}

        def fake_request(method: str, url: str, **kwargs: Any) -> None:
            observed["method"] = method
            observed["url"] = url
            observed["headers"] = kwargs.get("headers", {})

        client.bind(request_id="rid-1").httpx_request(
            fake_request,
            method="GET",
            url="http://downstream/path",
        )

        assert observed["method"] == "GET"
        assert observed["headers"].get("x-request-id") == "rid-1"

    def test_explicit_header_not_overwritten(self, make_client) -> None:
        client = make_client(enable_tracing=False)
        observed: dict[str, Any] = {}

        def fake_request(method: str, url: str, **kwargs: Any) -> None:
            observed["headers"] = kwargs.get("headers", {})

        client.bind(request_id="from-context").httpx_request(
            fake_request,
            method="GET",
            url="http://downstream/path",
            headers={"x-request-id": "explicit"},
        )

        assert observed["headers"]["x-request-id"] == "explicit"


class TestTracerProviderGuard:
    def test_existing_provider_reused_instead_of_clobbered(
        self,
        monkeypatch: pytest.MonkeyPatch,
        make_client,
    ) -> None:
        try:
            from opentelemetry.sdk.trace import TracerProvider
        except ImportError:
            pytest.skip("OpenTelemetry SDK not installed.")

        class _RecordCapture(logging.Handler):
            def __init__(self) -> None:
                super().__init__(level=logging.DEBUG)
                self.records: list[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        existing_provider = TracerProvider()
        monkeypatch.setattr(
            client_module.trace, "get_tracer_provider", lambda: existing_provider
        )

        set_calls: list[Any] = []
        monkeypatch.setattr(
            client_module.trace, "set_tracer_provider", lambda provider: set_calls.append(provider)
        )

        logger = logging.getLogger("observability.tracer-reuse-svc")
        capture = _RecordCapture()
        logger.addHandler(capture)

        try:
            client = make_client(
                service_name="tracer-reuse-svc",
                enable_tracing=True,
                enable_mlflow=False,
            )

            assert set_calls == []
            assert client._tracer is not None
            assert "tracer_provider_already_installed" in [
                record.getMessage() for record in capture.records
            ]
        finally:
            logger.removeHandler(capture)


class TestOtelUnavailableWarning:
    def test_warns_when_tracing_requested_but_otel_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        make_client,
    ) -> None:
        monkeypatch.setattr(client_module, "_OTEL_AVAILABLE", False)

        class _RecordCapture(logging.Handler):
            def __init__(self) -> None:
                super().__init__(level=logging.DEBUG)
                self.records: list[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        # Attach the capture before constructing the client so the warning lands on it.
        # We need a logger handle, which only exists after __init__. To capture the init-time
        # warning, attach to the named logger directly.
        logger = logging.getLogger("observability.otel-warning-svc")
        capture = _RecordCapture()
        logger.addHandler(capture)

        client = make_client(
            service_name="otel-warning-svc",
            enable_tracing=True,
            enable_mlflow=False,
        )

        try:
            messages = [record.getMessage() for record in capture.records]
            assert "tracing_dependency_missing" in messages
            assert client._tracer is None
        finally:
            logger.removeHandler(capture)
