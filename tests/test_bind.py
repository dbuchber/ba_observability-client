"""Tests for BoundObservabilityClient immutability and context merging."""

from __future__ import annotations

import logging
from typing import Any

from observability_client import ObservabilityClient
from observability_client.client import BoundObservabilityClient


class _RecordCapture(logging.Handler):
    """Capture LogRecord instances so tests can inspect ``ctx`` payloads."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _attach_capture(client: ObservabilityClient) -> _RecordCapture:
    handler = _RecordCapture()
    client._logger.addHandler(handler)
    return handler


class TestDeepMerge:
    def test_override_wins_on_scalar(self) -> None:
        merged = ObservabilityClient._deep_merge_dicts({"a": 1}, {"a": 2})
        assert merged == {"a": 2}

    def test_recursive_merge_on_nested_dicts(self) -> None:
        merged = ObservabilityClient._deep_merge_dicts(
            {"req": {"id": "abc", "method": "GET"}},
            {"req": {"id": "xyz"}},
        )
        assert merged == {"req": {"id": "xyz", "method": "GET"}}

    def test_does_not_mutate_inputs(self) -> None:
        base = {"req": {"id": "abc"}}
        override = {"req": {"id": "xyz"}}

        ObservabilityClient._deep_merge_dicts(base, override)

        assert base == {"req": {"id": "abc"}}
        assert override == {"req": {"id": "xyz"}}

    def test_override_replaces_dict_with_scalar(self) -> None:
        merged = ObservabilityClient._deep_merge_dicts({"x": {"nested": 1}}, {"x": "scalar"})
        assert merged == {"x": "scalar"}


class TestBindImmutability:
    def test_bind_returns_new_wrapper_instance(self, make_client) -> None:
        client = make_client()

        scoped_a = client.bind(a=1)
        scoped_b = scoped_a.bind(b=2)

        assert isinstance(scoped_a, BoundObservabilityClient)
        assert isinstance(scoped_b, BoundObservabilityClient)
        assert scoped_a is not scoped_b

    def test_bind_does_not_mutate_parent_context(self, make_client) -> None:
        client = make_client()

        scoped_a = client.bind(a=1)
        _scoped_b = scoped_a.bind(b=2)

        # Parent wrapper still sees only its own field, not the child's addition.
        assert scoped_a.context == {"a": 1}

    def test_child_context_includes_parent_fields(self, make_client) -> None:
        client = make_client()

        scoped = client.bind(a=1).bind(b=2)

        assert scoped.context == {"a": 1, "b": 2}

    def test_context_property_returns_defensive_copy(self, make_client) -> None:
        client = make_client()
        scoped = client.bind(nested={"id": "abc"})

        copy = scoped.context
        copy["nested"]["id"] = "MUTATED"

        # Re-reading must not reflect mutation of the returned copy.
        assert scoped.context == {"nested": {"id": "abc"}}

    def test_bind_strips_none_values(self, make_client) -> None:
        client = make_client()

        scoped = client.bind(real="value", absent=None)

        assert scoped.context == {"real": "value"}


class TestBoundLogEvent:
    def test_log_event_includes_bound_fields(self, make_client) -> None:
        client = make_client()
        capture = _attach_capture(client)

        client.bind(request_id="r-1", component="api").log_event("hello")

        assert len(capture.records) == 1
        ctx: dict[str, Any] = capture.records[0].ctx  # type: ignore[attr-defined]
        assert ctx["request_id"] == "r-1"
        assert ctx["component"] == "api"
        assert ctx["service"] == client.settings.service_name

    def test_bound_context_does_not_leak_to_subsequent_unbound_calls(self, make_client) -> None:
        client = make_client()
        capture = _attach_capture(client)

        client.bind(scope="inside").log_event("inside_event")
        client.log_event("outside_event")

        ctx_inside: dict[str, Any] = capture.records[0].ctx  # type: ignore[attr-defined]
        ctx_outside: dict[str, Any] = capture.records[1].ctx  # type: ignore[attr-defined]

        assert ctx_inside.get("scope") == "inside"
        assert "scope" not in ctx_outside

    def test_nested_bind_merges_context_for_logging(self, make_client) -> None:
        client = make_client()
        capture = _attach_capture(client)

        client.bind(request_id="r-1").bind(stage="retrieve").log_event("event")

        ctx: dict[str, Any] = capture.records[0].ctx  # type: ignore[attr-defined]
        assert ctx["request_id"] == "r-1"
        assert ctx["stage"] == "retrieve"
