"""Reusable observability client package public exports.

Compatibility contract:

- Primary import remains ``ObservabilityClient``.
- Optional alias ``TelemetryClient`` is provided for migration ergonomics.
- Existing import path stays stable:
  ``from observability_client import ObservabilityClient``.

Ergonomics:

- Optional package-level default client helpers are available.
- Local explicit client instantiation remains first-class.
"""

from __future__ import annotations

from threading import Lock
from typing import Any

from .client import BoundObservabilityClient, ObservabilityClient

_default_client_lock = Lock()
_default_client: ObservabilityClient | None = None


def get_default_client(**client_kwargs: Any) -> ObservabilityClient:
    """Return a shared process-local default client instance.

    If no default client exists yet, one is created lazily:

    - with ``client_kwargs`` via ``ObservabilityClient(...)``, or
    - without kwargs via ``ObservabilityClient.from_env()``.

    Args:
        **client_kwargs (Any): Optional constructor kwargs used only when
            creating the default instance for the first time.

    Returns:
        ObservabilityClient: Shared default client instance.
    """
    global _default_client

    with _default_client_lock:
        if _default_client is None:
            if client_kwargs:
                _default_client = ObservabilityClient(**client_kwargs)
            else:
                _default_client = ObservabilityClient.from_env()
        return _default_client


def reset_default_client() -> None:
    """Reset and close the shared default client instance.

    This is mainly useful in tests that need process-global state isolation.
    """
    global _default_client

    with _default_client_lock:
        existing = _default_client
        _default_client = None

    if existing is not None:
        existing.close()


def log_event(message: str, level: str = "info", **fields: Any) -> None:
    """Log one structured event through the default client.

    Args:
        message (str): Human-readable event message.
        level (str, optional): Log level token.
        **fields (Any): Additional structured event fields.
    """
    get_default_client().log_event(message, level=level, **fields)


def log_info(message: str, **fields: Any) -> None:
    """Log one info-level event through the default client.

    Args:
        message (str): Human-readable event message.
        **fields (Any): Additional structured event fields.
    """
    log_event(message, level="info", **fields)


def log_warning(message: str, **fields: Any) -> None:
    """Log one warning-level event through the default client.

    Args:
        message (str): Human-readable event message.
        **fields (Any): Additional structured event fields.
    """
    log_event(message, level="warning", **fields)


def log_error(message: str, **fields: Any) -> None:
    """Log one error-level event through the default client.

    Args:
        message (str): Human-readable event message.
        **fields (Any): Additional structured event fields.
    """
    log_event(message, level="error", **fields)

TelemetryClient = ObservabilityClient

__all__ = [
    "BoundObservabilityClient",
    "ObservabilityClient",
    "TelemetryClient",
    "get_default_client",
    "reset_default_client",
    "log_event",
    "log_info",
    "log_warning",
    "log_error",
]
