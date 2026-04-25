"""Reusable observability client for logs, traces, MLflow, and Loki push.

Public API contract:

- Primary public class is :class:`ObservabilityClient`.
- Existing helper constructors are preserved:
  :meth:`ObservabilityClient.quick_script_mode` and
  :meth:`ObservabilityClient.full_mode`.
- Environment factory constructor is available via
  :meth:`ObservabilityClient.from_env`.
- Configuration precedence is always:
  ``explicit kwargs > environment variables > hardcoded defaults``.

This module is intentionally backward compatible with existing import and
runtime usage while documenting contract decisions before deeper refactors.
"""

from __future__ import annotations

import contextlib
import contextvars
from copy import deepcopy
import json
import logging
import os
from queue import Empty, Full, Queue
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Literal, Mapping, cast
from urllib import error, request

try:
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None
try:
    from opentelemetry import propagate, trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    propagate = None
    trace = None
    OTLPSpanExporter = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    _OTEL_AVAILABLE = False

try:
    from opentelemetry.sdk.resources import DEPLOYMENT_ENVIRONMENT, SERVICE_NAME
except ImportError:  # pragma: no cover
    DEPLOYMENT_ENVIRONMENT = "deployment.environment"
    SERVICE_NAME = "service.name"


def git_commit() -> str:
    """Return the current git commit hash if available.

    Returns:
        str: Commit hash or ``"unknown"`` when not available.
    """
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    except Exception:  # noqa: BLE001
        return "unknown"


class JsonFormatter(logging.Formatter):
    """Format log records into compact one-line JSON payloads."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record into JSON.

        Args:
            record (logging.LogRecord): Log record instance.

        Returns:
            str: JSON log line.
        """
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        payload.update(getattr(record, "ctx", {}))
        return json.dumps(payload, separators=(",", ":"))


@dataclass(frozen=True)
class ObservabilitySettings:
    """Configuration values for the observability client.

    Attributes:
        service_name (str): Logical service name for logs and traces.
        env (str): Deployment environment (for example dev/staging/prod).
        profile (Literal["script", "service", "agent"]): Preset profile.
        experiment_name (str): MLflow experiment name.
        tracking_uri (str): MLflow tracking URI.
        otlp_endpoint (str): OTLP HTTP endpoint base URL.
        loki_push_endpoint (str): Loki push endpoint (via Alloy).
        loki_job (str): Stable Loki label used as broad source type.
        enable_mlflow (bool): Whether MLflow features are enabled.
        enable_tracing (bool): Whether OpenTelemetry tracing is enabled.
    """

    service_name: str
    env: str
    profile: Literal["script", "service", "agent"]
    experiment_name: str
    tracking_uri: str
    otlp_endpoint: str
    loki_push_endpoint: str
    loki_job: str
    enable_mlflow: bool
    enable_tracing: bool


@dataclass(frozen=True)
class _LokiQueueItem:
    """One queued Loki event for asynchronous background delivery.

    Attributes:
        message (str): Human-readable event message.
        level (str): Event level used for labels and payload metadata.
        fields (dict[str, object]): Additional structured event fields.
    """

    message: str
    level: str
    fields: dict[str, object]


class BoundObservabilityClient:
    """Immutable context-bound wrapper around :class:`ObservabilityClient`.

    The wrapper keeps a private bound context payload and applies it whenever
    logging or tracing helper methods are invoked. New context is added through
    :meth:`bind`, which returns a new wrapper instance instead of mutating the
    existing one.
    """

    def __init__(self, client: "ObservabilityClient", context: Mapping[str, Any]) -> None:
        """Initialize a bound client wrapper.

        Args:
            client (ObservabilityClient): Base observability client.
            context (Mapping[str, Any]): Bound context payload.
        """
        self._client = client
        self._context = ObservabilityClient._deep_merge_dicts({}, dict(context))

    @property
    def context(self) -> dict[str, Any]:
        """Return a defensive copy of the bound context.

        Returns:
            dict[str, Any]: Current bound context payload.
        """
        return ObservabilityClient._deep_merge_dicts({}, self._context)

    def bind(self, **ctx: Any) -> "BoundObservabilityClient":
        """Return a new wrapper with additional deep-merged bound context.

        Args:
            **ctx (Any): Context fields to add.

        Returns:
            BoundObservabilityClient: New immutable derived wrapper.
        """
        additional = {key: value for key, value in ctx.items() if value is not None}
        merged = ObservabilityClient._deep_merge_dicts(self._context, additional)
        return BoundObservabilityClient(self._client, merged)

    @contextlib.contextmanager
    def _activate(self) -> Iterator[None]:
        """Activate this wrapper's bound context for delegated calls.

        Yields:
            None: Active bound-context scope.
        """
        with self._client._bind_log_context(**self._context):
            yield

    def log_event(
        self,
        message: str,
        level: str = "info",
        *,
        push: bool = False,
        raise_on_error: bool = False,
        **fields: Any,
    ) -> None:
        """Log a structured event with bound context applied.

        Args:
            message (str): Event message.
            level (str, optional): Log level token.
            push (bool, optional): If ``True``, also push the event to Loki.
            raise_on_error (bool, optional): When ``push=True``, controls whether
                failed Loki delivery raises an exception.
            **fields (Any): Additional event fields.
        """
        with self._activate():
            self._client.log_event(
                message, level=level, push=push, raise_on_error=raise_on_error, **fields
            )

    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[trace.Span | None]:
        """Create a tracing span with this wrapper's context bound.

        Args:
            name (str): Span name.
            **attributes (Any): Span attributes.

        Yields:
            trace.Span | None: Active span or ``None`` when tracing is disabled.
        """
        with self._activate():
            with self._client.span(name, **attributes) as active_span:
                yield active_span

    @contextlib.contextmanager
    def fastapi_request_span(
        self,
        *,
        request: Any,
        span_name: str = "http.server.request",
        request_id: str | None = None,
        request_id_header: str = "x-request-id",
        **attributes: Any,
    ) -> Iterator[str]:
        """Create a FastAPI request span with wrapper context bound.

        Args:
            request (Any): FastAPI-like request object.
            span_name (str, optional): Server span name.
            request_id (str | None, optional): Explicit request ID.
            request_id_header (str, optional): Request ID header name.
            **attributes (Any): Additional span attributes.

        Yields:
            str: Resolved request ID.
        """
        with self._activate():
            with self._client.fastapi_request_span(
                request=request,
                span_name=span_name,
                request_id=request_id,
                request_id_header=request_id_header,
                **attributes,
            ) as resolved_request_id:
                yield resolved_request_id

    def httpx_request(
        self,
        request_func: Any,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        span_name: str = "http.client.request",
        request_id_header: str = "x-request-id",
        **kwargs: Any,
    ) -> Any:
        """Execute an ``httpx`` request with wrapper context bound.

        Args:
            request_func (Any): Callable compatible with ``httpx.Client.request``.
            method (str): HTTP method.
            url (str): Outbound URL.
            headers (dict[str, str] | None, optional): Outbound headers.
            span_name (str, optional): Client span name.
            request_id_header (str, optional): Request ID header name.
            **kwargs (Any): Additional request kwargs.

        Returns:
            Any: Response object from ``request_func``.
        """
        with self._activate():
            return self._client.httpx_request(
                request_func,
                method=method,
                url=url,
                headers=headers,
                span_name=span_name,
                request_id_header=request_id_header,
                **kwargs,
            )

    def __getattr__(self, name: str) -> Any:
        """Delegate unknown attributes to the wrapped base client.

        Callable attributes are wrapped so calls execute with this wrapper's
        bound context activated. Non-callable attributes are returned directly.

        Args:
            name (str): Attribute name.

        Returns:
            Any: Delegated attribute from underlying client.
        """
        delegated = getattr(self._client, name)
        if not callable(delegated):
            return delegated

        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            with self._activate():
                return delegated(*args, **kwargs)

        return _wrapped


class ObservabilityClient:
    """Provide one reusable API for logs, traces, MLflow, and Loki push.

    Compatibility guarantees for current consumers:

    - Keep this class as the primary public API.
    - Preserve existing methods (e.g. ``log_event``,
      ``start_run``, ``span``, ``close``).
    - Keep helper constructors as additive convenience APIs.
    - Use profile presets as config defaults, not separate class trees.

    Configuration resolution always follows:
    ``explicit kwargs > environment variables > defaults``.
    """

    _PROFILE_DEFAULTS: dict[str, dict[str, bool]] = {
        "script": {"enable_mlflow": False, "enable_tracing": False},
        "service": {"enable_mlflow": True, "enable_tracing": True},
        "agent": {"enable_mlflow": True, "enable_tracing": True},
    }
    _LOKI_QUEUE_MAXSIZE = 2000
    _LOKI_REQUEST_TIMEOUT_SECONDS = 1.5
    _LOKI_DROP_WARNING_INTERVAL_SECONDS = 30.0
    _LOKI_WORKER_IDLE_TIMEOUT_SECONDS = 0.2
    _LOKI_WORKER_JOIN_TIMEOUT_SECONDS = 1.0
    _DEFAULT_REQUEST_ID_HEADER = "x-request-id"

    def __init__(
        self,
        *,
        service_name: str,
        env: str,
        profile: Literal["script", "service", "agent"] = "service",
        experiment_name: str = "rag-kg-thesis",
        tracking_uri: str | None = None,
        otlp_endpoint: str | None = None,
        loki_push_endpoint: str | None = None,
        loki_job: str = "host-python",
        enable_mlflow: bool | None = None,
        enable_tracing: bool | None = None,
    ) -> None:
        """Initialize the observability client.

        Resolution precedence for optional settings is:
        ``explicit kwargs > environment variables > defaults``.

        Args:
            service_name (str): Logical service name.
            env (str): Deployment environment (for example dev/staging/prod).
            profile (Literal["script", "service", "agent"], optional):
                Profile preset controlling default feature toggles.
            experiment_name (str, optional): MLflow experiment name.
            tracking_uri (str | None, optional): MLflow tracking URI.
                Falls back to ``MLFLOW_TRACKING_URI`` and then
                ``http://localhost:5000``.
            otlp_endpoint (str | None, optional): OTLP HTTP base endpoint.
                Falls back to ``OTEL_EXPORTER_OTLP_ENDPOINT`` and then
                ``http://localhost:4318``.
            loki_push_endpoint (str | None, optional): Loki push endpoint URL.
                Falls back to ``ALLOY_LOG_PUSH_ENDPOINT`` and then
                ``http://localhost:9999/loki/api/v1/push``.
            loki_job (str, optional): Loki label for broad source type.
            enable_mlflow (bool | None, optional): Explicit MLflow override.
                If ``None``, profile preset value is used.
            enable_tracing (bool | None, optional): Explicit tracing override.
                If ``None``, profile preset value is used.

        Raises:
            ValueError: If required values are empty or profile is invalid.
        """
        normalized_service_name = self._require_non_empty(service_name, "service_name")
        normalized_env = self._require_non_empty(env, "env")
        normalized_profile = self._require_profile(profile)
        resolved_enable_mlflow, resolved_enable_tracing = self._resolve_profile_toggles(
            profile=normalized_profile,
            enable_mlflow=enable_mlflow,
            enable_tracing=enable_tracing,
        )

        self.settings = ObservabilitySettings(
            service_name=normalized_service_name,
            env=normalized_env,
            profile=normalized_profile,
            experiment_name=experiment_name,
            tracking_uri=tracking_uri
            or os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"),
            otlp_endpoint=otlp_endpoint
            or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
            loki_push_endpoint=loki_push_endpoint
            or os.getenv("ALLOY_LOG_PUSH_ENDPOINT", "http://localhost:9999/loki/api/v1/push"),
            loki_job=loki_job,
            enable_mlflow=resolved_enable_mlflow,
            enable_tracing=resolved_enable_tracing,
        )
        if self.settings.enable_mlflow and mlflow is None:
            raise RuntimeError(
                "MLflow support is enabled but 'mlflow' is not installed. "
                "Install mlflow or disable MLflow with enable_mlflow=False/profile='script'."
            )
        self._logger = self._configure_logger()
        self._run_id: str | None = None
        self._bound_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
            f"observability_bound_context_{self.settings.service_name}",
            default={},
        )
        if self.settings.enable_tracing and not _OTEL_AVAILABLE:
            self.log_event(
                "tracing_dependency_missing",
                level="warning",
                note=(
                    "OpenTelemetry packages are not installed. "
                    "Tracing is disabled; structured logging continues."
                ),
            )
        self._tracer = (
            self._configure_tracer()
            if self.settings.enable_tracing and _OTEL_AVAILABLE
            else None
        )
        self._loki_queue: Queue[_LokiQueueItem] = Queue(maxsize=self._LOKI_QUEUE_MAXSIZE)
        self._loki_worker_stop = threading.Event()
        self._loki_worker_thread: threading.Thread | None = None
        self._loki_state_lock = threading.Lock()
        self._loki_dropped_events_count = 0
        self._loki_last_drop_warning_monotonic = 0.0
        self._loki_missing_endpoint_warned = False
        self._start_loki_worker()

    @staticmethod
    def _require_non_empty(value: str | None, field_name: str) -> str:
        """Return a stripped non-empty string.

        Args:
            value (str | None): Input value.
            field_name (str): Field name for error messages.

        Returns:
            str: Stripped value.

        Raises:
            ValueError: If value is empty after stripping.
        """
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be a non-empty string.")

        cleaned = value.strip()
        if not cleaned:
            raise ValueError(f"{field_name} must be a non-empty string.")
        return cleaned

    @classmethod
    def _require_profile(
        cls, profile: Literal["script", "service", "agent"] | str | None
    ) -> Literal["script", "service", "agent"]:
        """Validate and normalize a profile value.

        Args:
            profile (Literal["script", "service", "agent"] | str | None):
                Candidate profile value.

        Returns:
            Literal["script", "service", "agent"]: Normalized profile.

        Raises:
            ValueError: If profile is unsupported.
        """
        if not isinstance(profile, str):
            valid = ", ".join(sorted(cls._PROFILE_DEFAULTS))
            raise ValueError(f"profile must be one of: {valid}.")

        normalized = profile.strip().lower()
        if normalized not in cls._PROFILE_DEFAULTS:
            valid = ", ".join(sorted(cls._PROFILE_DEFAULTS))
            raise ValueError(f"profile must be one of: {valid}.")
        return cast(Literal["script", "service", "agent"], normalized)

    @classmethod
    def _resolve_profile_toggles(
        cls,
        *,
        profile: Literal["script", "service", "agent"],
        enable_mlflow: bool | None,
        enable_tracing: bool | None,
    ) -> tuple[bool, bool]:
        """Resolve toggle values from preset defaults and explicit overrides.

        Args:
            profile (Literal["script", "service", "agent"]): Profile preset.
            enable_mlflow (bool | None): Explicit MLflow override.
            enable_tracing (bool | None): Explicit tracing override.

        Returns:
            tuple[bool, bool]: Final ``(enable_mlflow, enable_tracing)``.
        """
        preset = cls._PROFILE_DEFAULTS[profile]
        return (
            preset["enable_mlflow"] if enable_mlflow is None else enable_mlflow,
            preset["enable_tracing"] if enable_tracing is None else enable_tracing,
        )

    @staticmethod
    def _first_non_empty(*values: str | None) -> str | None:
        """Return the first non-empty stripped string.

        Args:
            *values (str | None): Candidate values.

        Returns:
            str | None: First non-empty value, if available.
        """
        for value in values:
            if value is None:
                continue
            cleaned = value.strip()
            if cleaned:
                return cleaned
        return None

    @staticmethod
    def _parse_bool(value: str | None) -> bool | None:
        """Parse a boolean value from environment-style tokens.

        Args:
            value (str | None): Raw value.

        Returns:
            bool | None: Parsed bool, or ``None`` when not set.

        Raises:
            ValueError: If value is not a supported boolean token.
        """
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"1", "true", "yes", "on", "y"}:
            return True
        if normalized in {"0", "false", "no", "off", "n"}:
            return False
        raise ValueError(
            "Boolean env value must be one of: 1/0, true/false, yes/no, on/off."
        )

    @classmethod
    def from_env(
        cls,
        *,
        service_name: str | None = None,
        env: str | None = None,
        profile: Literal["script", "service", "agent"] | None = None,
        experiment_name: str | None = None,
        tracking_uri: str | None = None,
        otlp_endpoint: str | None = None,
        loki_push_endpoint: str | None = None,
        loki_job: str | None = None,
        enable_mlflow: bool | None = None,
        enable_tracing: bool | None = None,
    ) -> "ObservabilityClient":
        """Create a client from environment variables.

        Precedence is always ``explicit args > env vars > defaults``.

        Args:
            service_name (str | None, optional): Service name override.
            env (str | None, optional): Deployment environment override.
            profile (Literal["script", "service", "agent"] | None, optional):
                Profile override.
            experiment_name (str | None, optional): Experiment name override.
            tracking_uri (str | None, optional): Tracking URI override.
            otlp_endpoint (str | None, optional): OTLP endpoint override.
            loki_push_endpoint (str | None, optional): Loki endpoint override.
            loki_job (str | None, optional): Loki job override.
            enable_mlflow (bool | None, optional): MLflow override.
            enable_tracing (bool | None, optional): Tracing override.

        Returns:
            ObservabilityClient: Configured client instance.

        Raises:
            ValueError: If required values are missing or invalid.
        """
        resolved_service_name = (
            service_name
            if service_name is not None
            else cls._first_non_empty(
                os.getenv("OBSERVABILITY_SERVICE_NAME"),
                os.getenv("SERVICE_NAME"),
            )
        )
        resolved_env = (
            env
            if env is not None
            else cls._first_non_empty(
                os.getenv("OBSERVABILITY_ENV"),
                os.getenv("ENV"),
            )
        )
        resolved_profile = (
            profile
            if profile is not None
            else cls._first_non_empty(
                os.getenv("OBSERVABILITY_PROFILE"),
                os.getenv("TELEMETRY_PROFILE"),
                "service",
            )
        )
        resolved_experiment_name = (
            experiment_name
            if experiment_name is not None
            else cls._first_non_empty(
                os.getenv("EXPERIMENT_NAME"),
                os.getenv("MLFLOW_EXPERIMENT_NAME"),
                "rag-kg-thesis",
            )
        )
        resolved_tracking_uri = (
            tracking_uri
            if tracking_uri is not None
            else cls._first_non_empty(os.getenv("MLFLOW_TRACKING_URI"))
        )
        resolved_otlp_endpoint = (
            otlp_endpoint
            if otlp_endpoint is not None
            else cls._first_non_empty(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
        )
        resolved_loki_push_endpoint = (
            loki_push_endpoint
            if loki_push_endpoint is not None
            else cls._first_non_empty(
                os.getenv("ALLOY_LOG_PUSH_ENDPOINT"),
                os.getenv("LOKI_PUSH_ENDPOINT"),
                os.getenv("LOKI_URL"),
            )
        )
        resolved_loki_job = (
            loki_job
            if loki_job is not None
            else cls._first_non_empty(
                os.getenv("OBSERVABILITY_LOKI_JOB"),
                os.getenv("LOKI_JOB"),
                "host-python",
            )
        )

        resolved_enable_mlflow = enable_mlflow
        if resolved_enable_mlflow is None:
            resolved_enable_mlflow = cls._parse_bool(
                cls._first_non_empty(
                    os.getenv("OBSERVABILITY_ENABLE_MLFLOW"),
                    os.getenv("ENABLE_MLFLOW"),
                )
            )

        resolved_enable_tracing = enable_tracing
        if resolved_enable_tracing is None:
            resolved_enable_tracing = cls._parse_bool(
                cls._first_non_empty(
                    os.getenv("OBSERVABILITY_ENABLE_TRACING"),
                    os.getenv("ENABLE_TRACING"),
                )
            )

        if resolved_service_name is None:
            raise ValueError(
                "service_name is required. Pass service_name or set "
                "OBSERVABILITY_SERVICE_NAME/SERVICE_NAME."
            )
        if resolved_env is None:
            raise ValueError("env is required. Pass env or set OBSERVABILITY_ENV/ENV.")

        return cls(
            service_name=resolved_service_name,
            env=resolved_env,
            profile=resolved_profile,
            experiment_name=resolved_experiment_name,
            tracking_uri=resolved_tracking_uri,
            otlp_endpoint=resolved_otlp_endpoint,
            loki_push_endpoint=resolved_loki_push_endpoint,
            loki_job=resolved_loki_job,
            enable_mlflow=resolved_enable_mlflow,
            enable_tracing=resolved_enable_tracing,
        )

    @classmethod
    def quick_script_mode(
        cls,
        *,
        service_name: str,
        env: str | None = None,
        loki_push_endpoint: str | None = None,
        loki_job: str = "host-python",
    ) -> "ObservabilityClient":
        """Create a lightweight client for simple script logging.

        This mode is optimized for helper scripts that only need direct Loki
        push events and structured local logs.

        Precedence remains ``explicit kwargs > environment variables > defaults``
        for optional endpoint settings.

        Args:
            service_name (str): Logical service/script name.
            env (str | None, optional): Deployment environment. Falls back to
                ``OBSERVABILITY_ENV``/``ENV`` and then ``"dev"``.
            loki_push_endpoint (str | None, optional): Loki push endpoint URL.
            loki_job (str, optional): Loki job label value.

        Returns:
            ObservabilityClient: Client with MLflow and tracing disabled.
        """
        return cls(
            service_name=service_name,
            env=env
            or cls._first_non_empty(os.getenv("OBSERVABILITY_ENV"), os.getenv("ENV"), "dev")
            or "dev",
            profile="script",
            loki_push_endpoint=loki_push_endpoint,
            loki_job=loki_job,
            enable_mlflow=False,
            enable_tracing=False,
        )

    @classmethod
    def full_mode(
        cls,
        *,
        service_name: str,
        env: str | None = None,
        experiment_name: str = "rag-kg-thesis",
        tracking_uri: str | None = None,
        otlp_endpoint: str | None = None,
        loki_push_endpoint: str | None = None,
        loki_job: str = "host-python",
    ) -> "ObservabilityClient":
        """Create a full-featured client for runs, traces, and logs.

        Precedence remains ``explicit kwargs > environment variables > defaults``
        for optional endpoint settings.

        Args:
            service_name (str): Logical service/script name.
            env (str | None, optional): Deployment environment. Falls back to
                ``OBSERVABILITY_ENV``/``ENV`` and then ``"dev"``.
            experiment_name (str, optional): MLflow experiment name.
            tracking_uri (str | None, optional): MLflow tracking URI.
            otlp_endpoint (str | None, optional): OTLP HTTP base endpoint.
            loki_push_endpoint (str | None, optional): Loki push endpoint URL.
            loki_job (str, optional): Loki job label value.

        Returns:
            ObservabilityClient: Full observability client.
        """
        return cls(
            service_name=service_name,
            env=env
            or cls._first_non_empty(os.getenv("OBSERVABILITY_ENV"), os.getenv("ENV"), "dev")
            or "dev",
            profile="service",
            experiment_name=experiment_name,
            tracking_uri=tracking_uri,
            otlp_endpoint=otlp_endpoint,
            loki_push_endpoint=loki_push_endpoint,
            loki_job=loki_job,
            enable_mlflow=True,
            enable_tracing=True,
        )

    def _configure_logger(self) -> logging.Logger:
        """Create a JSON stdout logger.

        Returns:
            logging.Logger: Configured logger.
        """
        logger = logging.getLogger(f"observability.{self.settings.service_name}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        if logger.handlers:
            return logger

        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        return logger

    def _configure_tracer(self) -> trace.Tracer:
        """Configure OpenTelemetry tracing for this service.

        Returns:
            trace.Tracer: Tracer instance.

        Raises:
            RuntimeError: If OpenTelemetry dependencies are not installed.
        """
        if not _OTEL_AVAILABLE:
            raise RuntimeError(
                "Tracing is enabled but OpenTelemetry dependencies are not installed."
            )

        resource = Resource.create(
            {
                SERVICE_NAME: self.settings.service_name,
                DEPLOYMENT_ENVIRONMENT: self.settings.env,
                "deployment.environment.name": self.settings.env,
            }
        )
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(
            endpoint=f"{self.settings.otlp_endpoint.rstrip('/')}/v1/traces"
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        return trace.get_tracer(self.settings.service_name)

    def _to_unix_ns_timestamp(self) -> str:
        """Return the current UTC time as a Unix nanoseconds string.

        Returns:
            str: Current timestamp in Unix nanoseconds.
        """
        return str(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))

    @staticmethod
    def _deep_merge_dicts(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
        """Deep-merge two dictionaries without mutating either input.

        Values from ``override`` take precedence. Nested dictionaries are merged
        recursively.

        Args:
            base (Mapping[str, Any]): Base dictionary.
            override (Mapping[str, Any]): Values overriding ``base``.

        Returns:
            dict[str, Any]: Deep-merged dictionary.
        """
        merged: dict[str, Any] = deepcopy(dict(base))
        for key, override_value in override.items():
            existing_value = merged.get(key)
            if isinstance(existing_value, dict) and isinstance(override_value, Mapping):
                merged[key] = ObservabilityClient._deep_merge_dicts(existing_value, override_value)
                continue
            merged[key] = deepcopy(override_value)
        return merged

    @staticmethod
    def _normalize_carrier_headers(headers: Any) -> dict[str, str]:
        """Normalize header carriers to a lower-case string dictionary.

        Args:
            headers (Any): Header carrier with ``items()`` support.

        Returns:
            dict[str, str]: Normalized lower-case headers.
        """
        if not hasattr(headers, "items"):
            return {}

        normalized: dict[str, str] = {}
        for raw_key, raw_value in headers.items():
            key = str(raw_key).strip().lower()
            if not key:
                continue
            normalized[key] = str(raw_value)
        return normalized

    @classmethod
    def _resolve_request_id(
        cls,
        *,
        headers: dict[str, str],
        request_id: str | None,
        request_id_header: str,
    ) -> str:
        """Resolve request identifier from explicit arg, header, or generated UUID.

        Args:
            headers (dict[str, str]): Normalized lower-case headers.
            request_id (str | None): Explicit request ID override.
            request_id_header (str): Request ID header name.

        Returns:
            str: Resolved request ID.
        """
        if isinstance(request_id, str) and request_id.strip():
            return request_id.strip()

        resolved_header = request_id_header.strip().lower() or cls._DEFAULT_REQUEST_ID_HEADER
        header_value = headers.get(resolved_header, "").strip()
        if header_value:
            return header_value

        return uuid.uuid4().hex

    def _current_trace_correlation_fields(self) -> dict[str, str]:
        """Return trace correlation fields for the active span when available.

        Returns:
            dict[str, str]: ``trace_id`` and ``span_id`` in hex format.
        """
        if trace is None:
            return {}

        active_span = trace.get_current_span()
        span_context = active_span.get_span_context() if active_span else None
        if not span_context or not span_context.is_valid:
            return {}

        return {
            "trace_id": trace.format_trace_id(span_context.trace_id),
            "span_id": trace.format_span_id(span_context.span_id),
        }

    def _merge_contextual_fields(self, fields: dict[str, Any]) -> dict[str, Any]:
        """Merge bound context and active trace correlation into event fields.

        Args:
            fields (dict[str, Any]): Explicit event fields.

        Returns:
            dict[str, Any]: Merged fields with contextual defaults applied.
        """
        merged = self._deep_merge_dicts(self._get_bound_log_context(), fields)
        for key, value in self._current_trace_correlation_fields().items():
            merged.setdefault(key, value)
        return merged

    def _get_bound_log_context(self) -> dict[str, Any]:
        """Return a copy of currently bound log context fields.

        Returns:
            dict[str, Any]: Bound contextual fields.
        """
        return deepcopy(dict(self._bound_log_context.get()))

    @contextlib.contextmanager
    def _bind_log_context(self, **fields: Any) -> Iterator[None]:
        """Temporarily bind contextual fields for logs in current execution context.

        Args:
            **fields (Any): Context fields to bind for nested operations.

        Yields:
            None: Context manager scope.
        """
        current = self._get_bound_log_context()
        additional = {key: value for key, value in fields.items() if value is not None}
        token = self._bound_log_context.set(self._deep_merge_dicts(current, additional))
        try:
            yield
        finally:
            self._bound_log_context.reset(token)

    def bind(self, **ctx: Any) -> BoundObservabilityClient:
        """Return an immutable context-bound client wrapper.

        Bound context is applied to log and tracing helper calls issued through
        the returned wrapper. Calling ``bind`` again on that wrapper returns a new
        derived wrapper (no shared mutable state).

        Args:
            **ctx (Any): Context fields to bind.

        Returns:
            BoundObservabilityClient: Derived immutable wrapper.
        """
        cleaned = {key: value for key, value in ctx.items() if value is not None}
        return BoundObservabilityClient(self, cleaned)

    @contextlib.contextmanager
    def fastapi_request_span(
        self,
        *,
        request: Any,
        span_name: str = "http.server.request",
        request_id: str | None = None,
        request_id_header: str = _DEFAULT_REQUEST_ID_HEADER,
        **attributes: Any,
    ) -> Iterator[str]:
        """Create a server span from FastAPI request headers with W3C extraction.

        This helper extracts incoming W3C propagation headers (``traceparent`` /
        ``tracestate``), starts a request span with the extracted parent context,
        and binds a ``request_id`` into log context for the duration of the span.

        Args:
            request (Any): FastAPI-like request object exposing ``headers``.
            span_name (str, optional): Span name for inbound request handling.
            request_id (str | None, optional): Explicit request identifier.
            request_id_header (str, optional): Header key used for request ID.
            **attributes (Any): Additional span attributes.

        Yields:
            str: Resolved request ID bound to this request context.
        """
        incoming_headers = self._normalize_carrier_headers(getattr(request, "headers", {}))
        extracted_context = propagate.extract(incoming_headers) if propagate else None
        resolved_request_id = self._resolve_request_id(
            headers=incoming_headers,
            request_id=request_id,
            request_id_header=request_id_header,
        )

        span_attributes = {
            "request_id": resolved_request_id,
            **attributes,
        }
        method = getattr(request, "method", None)
        if method:
            span_attributes.setdefault("http.method", str(method))

        request_url = getattr(request, "url", None)
        path = getattr(request_url, "path", None)
        if path:
            span_attributes.setdefault("http.route", str(path))

        with self._bind_log_context(request_id=resolved_request_id):
            if not self._tracer:
                yield resolved_request_id
                return

            with self._tracer.start_as_current_span(
                span_name,
                context=extracted_context,
                kind=trace.SpanKind.SERVER,
            ) as active_span:
                for key, value in span_attributes.items():
                    if value is not None:
                        active_span.set_attribute(key, value)
                yield resolved_request_id

    def inject_trace_headers(self, headers: dict[str, str] | None = None) -> dict[str, str]:
        """Return headers with W3C trace context injected from current context.

        Args:
            headers (dict[str, str] | None, optional): Existing outbound headers.

        Returns:
            dict[str, str]: Headers including injected tracing context.
        """
        carrier = dict(headers or {})
        if propagate:
            propagate.inject(carrier)
        return carrier

    def httpx_request(
        self,
        request_func: Any,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        span_name: str = "http.client.request",
        request_id_header: str = _DEFAULT_REQUEST_ID_HEADER,
        **kwargs: Any,
    ) -> Any:
        """Wrap an ``httpx`` request call with client span + header injection.

        Typical usage:
        ``client.httpx_request(httpx_client.request, method="GET", url="...")``.

        Args:
            request_func (Any): Callable compatible with ``httpx.Client.request``.
            method (str): HTTP method.
            url (str): Outbound URL.
            headers (dict[str, str] | None, optional): Outbound headers.
            span_name (str, optional): Client span name.
            request_id_header (str, optional): Header key used for request ID.
            **kwargs (Any): Additional kwargs forwarded to ``request_func``.

        Returns:
            Any: Result returned by ``request_func``.
        """
        outgoing_headers = dict(headers or {})
        if "headers" in kwargs and kwargs["headers"]:
            extra_headers = kwargs.pop("headers")
            outgoing_headers = {**dict(extra_headers), **outgoing_headers}

        bound_request_id = self._get_bound_log_context().get("request_id")
        header_key = request_id_header.strip() or self._DEFAULT_REQUEST_ID_HEADER
        normalized_keys = {str(key).lower() for key in outgoing_headers}
        if bound_request_id and header_key.lower() not in normalized_keys:
            outgoing_headers[header_key] = str(bound_request_id)

        if not self._tracer:
            outgoing_headers = self.inject_trace_headers(outgoing_headers)
            return request_func(method, url, headers=outgoing_headers, **kwargs)

        with self._tracer.start_as_current_span(span_name, kind=trace.SpanKind.CLIENT) as active_span:
            active_span.set_attribute("http.method", method)
            active_span.set_attribute("http.url", url)
            if bound_request_id:
                active_span.set_attribute("request_id", str(bound_request_id))

            outgoing_headers = self.inject_trace_headers(outgoing_headers)
            return request_func(method, url, headers=outgoing_headers, **kwargs)

    def _build_loki_body(self, message: str, level: str, fields: dict[str, object]) -> dict[str, Any]:
        """Build a Loki push payload body from event fields.

        Args:
            message (str): Human-readable message text.
            level (str): Log level label value.
            fields (dict[str, object]): Additional structured event fields.

        Returns:
            dict[str, Any]: Loki push API request body.
        """
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "service": self.settings.service_name,
            "env": self.settings.env,
            "profile": self.settings.profile,
            **fields,
        }
        return {
            "streams": [
                {
                    "stream": {
                        "job": self.settings.loki_job,
                        "service": self.settings.service_name,
                        "env": self.settings.env,
                        "host": socket.gethostname(),
                        "level": level,
                    },
                    "values": [
                        [
                            self._to_unix_ns_timestamp(),
                            json.dumps(payload, separators=(",", ":")),
                        ]
                    ],
                }
            ]
        }

    def _warn_missing_loki_endpoint_once(self) -> None:
        """Emit one loud warning when Loki endpoint is missing."""
        with self._loki_state_lock:
            if self._loki_missing_endpoint_warned:
                return
            self._loki_missing_endpoint_warned = True

        self.log_event(
            "loki_push_endpoint_missing",
            level="warning",
            action="stdout_logging_only",
            note="Set ALLOY_LOG_PUSH_ENDPOINT or pass loki_push_endpoint.",
        )

    def _post_loki_body(self, body: dict[str, Any], endpoint: str, raise_on_error: bool) -> bool:
        """Send one prepared Loki body over HTTP.

        Args:
            body (dict[str, Any]): Loki push body.
            endpoint (str): Loki push endpoint URL.
            raise_on_error (bool): Whether to raise on failed delivery.

        Returns:
            bool: ``True`` when delivery succeeded, otherwise ``False``.

        Raises:
            RuntimeError: If delivery fails and ``raise_on_error`` is true.
        """
        req = request.Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self._LOKI_REQUEST_TIMEOUT_SECONDS):
                return True
        except (error.URLError, TimeoutError, OSError, ValueError) as exc:
            if raise_on_error:
                raise RuntimeError(
                    f"Failed to push log to Loki endpoint '{endpoint}'."
                ) from exc
            return False

    def _push_log_sync(
        self,
        *,
        message: str,
        level: str,
        fields: dict[str, object],
        raise_on_error: bool,
    ) -> bool:
        """Synchronously send one log event to Loki.

        Args:
            message (str): Human-readable message text.
            level (str): Log level label value.
            fields (dict[str, object]): Additional structured event fields.
            raise_on_error (bool): Whether failed delivery raises.

        Returns:
            bool: ``True`` on success, otherwise ``False``.

        Raises:
            RuntimeError: If endpoint is missing or delivery fails in strict mode.
        """
        endpoint = self.settings.loki_push_endpoint.strip()
        if not endpoint:
            self._warn_missing_loki_endpoint_once()
            if raise_on_error:
                raise RuntimeError(
                    "Loki push endpoint is missing. "
                    "Set ALLOY_LOG_PUSH_ENDPOINT or pass loki_push_endpoint."
                )
            return False

        body = self._build_loki_body(message=message, level=level, fields=fields)
        return self._post_loki_body(body=body, endpoint=endpoint, raise_on_error=raise_on_error)

    def _maybe_warn_dropped_events(self) -> None:
        """Emit throttled warnings about dropped Loki events."""
        should_warn = False
        dropped_count = 0
        now = time.monotonic()

        with self._loki_state_lock:
            if (
                now - self._loki_last_drop_warning_monotonic
                >= self._LOKI_DROP_WARNING_INTERVAL_SECONDS
            ):
                self._loki_last_drop_warning_monotonic = now
                should_warn = True
                dropped_count = self._loki_dropped_events_count

        if should_warn:
            self.log_event(
                "loki_queue_full_dropping_events",
                level="warning",
                dropped_count=dropped_count,
                queue_maxsize=self._LOKI_QUEUE_MAXSIZE,
            )

    def _enqueue_loki_event(self, message: str, level: str, fields: dict[str, object]) -> None:
        """Enqueue one Loki event for asynchronous background delivery.

        Args:
            message (str): Human-readable message text.
            level (str): Log level label value.
            fields (dict[str, object]): Additional structured event fields.
        """
        try:
            self._loki_queue.put_nowait(
                _LokiQueueItem(message=message, level=level, fields=dict(fields))
            )
        except Full:
            with self._loki_state_lock:
                self._loki_dropped_events_count += 1
            self._maybe_warn_dropped_events()

    def _loki_worker_loop(self) -> None:
        """Run background delivery loop for queued Loki events."""
        while True:
            if self._loki_worker_stop.is_set() and self._loki_queue.empty():
                return

            try:
                item = self._loki_queue.get(timeout=self._LOKI_WORKER_IDLE_TIMEOUT_SECONDS)
            except Empty:
                continue

            try:
                self._push_log_sync(
                    message=item.message,
                    level=item.level,
                    fields=item.fields,
                    raise_on_error=False,
                )
            finally:
                self._loki_queue.task_done()

    def _start_loki_worker(self) -> None:
        """Start the asynchronous Loki delivery worker thread."""
        if self._loki_worker_thread and self._loki_worker_thread.is_alive():
            return

        self._loki_worker_thread = threading.Thread(
            target=self._loki_worker_loop,
            name=f"observability-loki-{self.settings.service_name}",
            daemon=True,
        )
        self._loki_worker_thread.start()

    def _stop_loki_worker(self) -> None:
        """Stop the asynchronous Loki delivery worker thread."""
        self._loki_worker_stop.set()
        if self._loki_worker_thread and self._loki_worker_thread.is_alive():
            self._loki_worker_thread.join(timeout=self._LOKI_WORKER_JOIN_TIMEOUT_SECONDS)

    def start_run(
        self,
        *,
        run_name: str | None = None,
        params: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> str:
        """Start an MLflow run and store its run ID.

        Args:
            run_name (str | None, optional): Optional run name.
            params (dict[str, Any] | None, optional): Params to log at startup.
            tags (dict[str, str] | None, optional): Tags to set at startup.

        Returns:
            str: Active MLflow run ID.

        Raises:
            RuntimeError: If MLflow support is disabled for this client.
        """
        if not self.settings.enable_mlflow:
            raise RuntimeError("MLflow is disabled in quick_script_mode.")

        mlflow.set_tracking_uri(self.settings.tracking_uri)
        mlflow.set_experiment(self.settings.experiment_name)
        run = mlflow.start_run(run_name=run_name)
        self._run_id = run.info.run_id

        default_tags = {
            "service_name": self.settings.service_name,
            "git_commit": git_commit(),
        }
        for key, value in {**default_tags, **(tags or {})}.items():
            mlflow.set_tag(key, value)

        if params:
            mlflow.log_params(params)

        self.log_event(
            "mlflow_run_started",
            run_id=self._run_id,
            experiment=self.settings.experiment_name,
        )
        return self._run_id

    @property
    def run_id(self) -> str | None:
        """Return the active MLflow run ID.

        Returns:
            str | None: Active run ID when a run is open.
        """
        return self._run_id

    def log_event(
        self,
        message: str,
        level: str = "info",
        *,
        push: bool = False,
        raise_on_error: bool = False,
        **fields: Any,
    ) -> None:
        """Log a structured JSON event to stdout and optionally push to Loki.

        Args:
            message (str): Human-readable event message.
            level (str, optional): Log level (info, warning, error, debug).
            push (bool, optional): If ``True``, also push the event to Loki.
            raise_on_error (bool, optional): When ``push=True``, controls whether
                failed Loki delivery raises an exception. Default is ``False``
                (non-blocking, silent failure).
            **fields (Any): Additional structured fields.
        """
        payload_fields = self._merge_contextual_fields(fields)

        payload = {
            "service": self.settings.service_name,
            "env": self.settings.env,
            "profile": self.settings.profile,
            **payload_fields,
        }
        if self._run_id and "run_id" not in payload:
            payload["run_id"] = self._run_id

        method = getattr(self._logger, level.lower(), self._logger.info)
        method(message, extra={"ctx": payload})

        if push:
            if raise_on_error:
                self._push_log_sync(
                    message=message,
                    level=level,
                    fields=payload_fields,
                    raise_on_error=True,
                )
            else:
                self._enqueue_loki_event(message=message, level=level, fields=payload_fields)

    def log_sparql(
        self,
        *,
        query_hash: str,
        stage: str,
        latency_ms: float,
        result_count: int | None = None,
        sparql_endpoint: str | None = None,
        **fields: Any,
    ) -> None:
        """Log a normalized SPARQL event payload.

        Args:
            query_hash (str): Hash of SPARQL query text.
            stage (str): Pipeline stage name.
            latency_ms (float): Query latency in milliseconds.
            result_count (int | None, optional): Number of rows or bindings.
            sparql_endpoint (str | None, optional): Endpoint URL.
            **fields (Any): Additional structured log fields.
        """
        self.log_event(
            "sparql_query",
            stage=stage,
            query_hash=query_hash,
            latency_ms=latency_ms,
            result_count=result_count,
            sparql_endpoint=sparql_endpoint,
            **fields,
        )

    def log_metrics(self, metrics: dict[str, float]) -> None:
        """Log numeric metrics to MLflow when enabled.

        Args:
            metrics (dict[str, float]): Metric name/value mapping.
        """
        if self.settings.enable_mlflow and metrics:
            mlflow.log_metrics(metrics)

    def log_params(self, params: dict[str, Any]) -> None:
        """Log parameters to MLflow when enabled.

        Args:
            params (dict[str, Any]): Parameter name/value mapping.
        """
        if self.settings.enable_mlflow and params:
            mlflow.log_params(params)

    def log_artifact(self, path: str) -> None:
        """Upload a local file as an MLflow artifact when enabled.

        Args:
            path (str): Artifact file path.
        """
        if self.settings.enable_mlflow:
            mlflow.log_artifact(path)

    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[trace.Span | None]:
        """Create a tracing span with optional attributes.

        Args:
            name (str): Span name.
            **attributes (Any): Span attributes.

        Yields:
            trace.Span | None: Active span, or ``None`` if tracing is disabled.
        """
        if not self._tracer:
            yield None
            return

        with self._tracer.start_as_current_span(name) as active_span:
            for key, value in attributes.items():
                if value is not None:
                    active_span.set_attribute(key, value)
            yield active_span

    def close(self) -> None:
        """End the active MLflow run and emit a closing event."""
        self._stop_loki_worker()
        if self.settings.enable_mlflow and self._run_id:
            self.log_event("mlflow_run_closed", run_id=self._run_id)
            mlflow.end_run()
            self._run_id = None
