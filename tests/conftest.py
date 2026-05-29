"""Shared fixtures for the observability_client test suite."""

from __future__ import annotations

import threading
from typing import Any, Callable, Iterator

import pytest

from observability_client import ObservabilityClient, reset_default_client
from observability_client import client as client_module

_OBSERVABILITY_ENV_VARS = (
    "OBSERVABILITY_SERVICE_NAME",
    "SERVICE_NAME",
    "OBSERVABILITY_ENV",
    "ENV",
    "OBSERVABILITY_PROFILE",
    "TELEMETRY_PROFILE",
    "EXPERIMENT_NAME",
    "MLFLOW_EXPERIMENT_NAME",
    "MLFLOW_TRACKING_URI",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "ALLOY_LOG_PUSH_ENDPOINT",
    "LOKI_PUSH_ENDPOINT",
    "LOKI_URL",
    "OBSERVABILITY_LOKI_JOB",
    "LOKI_JOB",
    "OBSERVABILITY_ENABLE_MLFLOW",
    "ENABLE_MLFLOW",
    "OBSERVABILITY_ENABLE_TRACING",
    "ENABLE_TRACING",
)


@pytest.fixture(autouse=True)
def _clean_observability_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every observability-related env var before each test."""
    for name in _OBSERVABILITY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _reset_default_client_state() -> Iterator[None]:
    """Ensure the package-level default client doesn't leak across tests."""
    reset_default_client()
    yield
    reset_default_client()


@pytest.fixture
def make_client() -> Iterator[Callable[..., ObservabilityClient]]:
    """Factory that creates ObservabilityClient instances and closes them on teardown."""
    created: list[ObservabilityClient] = []

    def _factory(**overrides: Any) -> ObservabilityClient:
        kwargs: dict[str, Any] = {
            "service_name": "test-service",
            "env": "test",
            "profile": "script",
        }
        kwargs.update(overrides)
        instance = ObservabilityClient(**kwargs)
        created.append(instance)
        return instance

    yield _factory

    for instance in created:
        try:
            instance.close()
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture
def loki_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Capture Loki POSTs without hitting the network.

    Returns a state dict with:

    - ``bodies``: list of ``(body, endpoint)`` tuples actually delivered.
    - ``fail``: set to True to make every delivery fail.
    - ``delivered``: ``threading.Event`` set after each successful delivery.
    - ``barrier``: optional ``threading.Event`` that blocks delivery until set.
    """
    state: dict[str, Any] = {
        "bodies": [],
        "fail": False,
        "delivered": threading.Event(),
        "barrier": None,
    }
    lock = threading.Lock()

    def _fake_post(
        self: ObservabilityClient,
        body: dict[str, Any],
        endpoint: str,
        raise_on_error: bool,
    ) -> bool:
        barrier = state["barrier"]
        if barrier is not None:
            barrier.wait()
        if state["fail"]:
            if raise_on_error:
                raise RuntimeError(f"Failed to push log to Loki endpoint '{endpoint}'.")
            return False
        with lock:
            state["bodies"].append((body, endpoint))
        state["delivered"].set()
        return True

    monkeypatch.setattr(client_module.ObservabilityClient, "_post_loki_body", _fake_post)
    return state
