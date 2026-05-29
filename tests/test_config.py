"""Tests for ObservabilityClient configuration resolution.

Covers the documented precedence ``explicit kwargs > env vars > defaults``,
profile presets, ``from_env`` validation, and the boolean env parser.
"""

from __future__ import annotations

import pytest

from observability_client import ObservabilityClient


class TestConstructorPrecedence:
    def test_explicit_kwargs_override_env(self, monkeypatch: pytest.MonkeyPatch, make_client) -> None:
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://from-env:5000")

        client = make_client(tracking_uri="http://explicit:5000")

        assert client.settings.tracking_uri == "http://explicit:5000"

    def test_env_used_when_no_kwarg(self, monkeypatch: pytest.MonkeyPatch, make_client) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-env:4318")

        client = make_client()

        assert client.settings.otlp_endpoint == "http://otel-env:4318"

    def test_defaults_applied_when_no_env_and_no_kwarg(self, make_client) -> None:
        client = make_client()

        assert client.settings.tracking_uri == "http://localhost:5000"
        assert client.settings.otlp_endpoint == "http://localhost:4318"
        assert client.settings.loki_push_endpoint == "http://localhost:9999/loki/api/v1/push"
        assert client.settings.loki_job == "host-python"

    def test_loki_endpoint_env_used(self, monkeypatch: pytest.MonkeyPatch, make_client) -> None:
        monkeypatch.setenv("ALLOY_LOG_PUSH_ENDPOINT", "http://alloy:3100/loki/api/v1/push")

        client = make_client()

        assert client.settings.loki_push_endpoint == "http://alloy:3100/loki/api/v1/push"


class TestRequiredFields:
    def test_empty_service_name_raises(self) -> None:
        with pytest.raises(ValueError, match="service_name"):
            ObservabilityClient(service_name="   ", env="dev", profile="script")

    def test_empty_env_raises(self) -> None:
        with pytest.raises(ValueError, match="env"):
            ObservabilityClient(service_name="svc", env="", profile="script")

    def test_invalid_profile_raises(self) -> None:
        with pytest.raises(ValueError, match="profile"):
            ObservabilityClient(service_name="svc", env="dev", profile="bogus")  # type: ignore[arg-type]


class TestProfilePresets:
    def test_script_disables_mlflow_and_tracing(self, make_client) -> None:
        client = make_client(profile="script")

        assert client.settings.enable_mlflow is False
        assert client.settings.enable_tracing is False

    def test_service_enables_both(self, make_client) -> None:
        client = make_client(profile="service", enable_tracing=False)

        # explicit override stays in effect for tracing; mlflow takes preset True
        assert client.settings.enable_mlflow is True
        assert client.settings.enable_tracing is False

    def test_explicit_override_beats_profile_preset(self, make_client) -> None:
        client = make_client(profile="service", enable_mlflow=False, enable_tracing=False)

        assert client.settings.enable_mlflow is False
        assert client.settings.enable_tracing is False


class TestFromEnv:
    def test_requires_service_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_ENV", "dev")

        with pytest.raises(ValueError, match="service_name"):
            ObservabilityClient.from_env()

    def test_requires_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_SERVICE_NAME", "svc")

        with pytest.raises(ValueError, match="env"):
            ObservabilityClient.from_env()

    def test_uses_primary_env_var_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_SERVICE_NAME", "primary")
        monkeypatch.setenv("OBSERVABILITY_ENV", "dev")

        client = ObservabilityClient.from_env(profile="script")
        try:
            assert client.settings.service_name == "primary"
            assert client.settings.env == "dev"
        finally:
            client.close()

    def test_alias_env_vars_used_when_primary_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SERVICE_NAME", "alias-svc")
        monkeypatch.setenv("ENV", "prod")

        client = ObservabilityClient.from_env(profile="script")
        try:
            assert client.settings.service_name == "alias-svc"
            assert client.settings.env == "prod"
        finally:
            client.close()

    def test_primary_env_var_beats_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_SERVICE_NAME", "primary")
        monkeypatch.setenv("SERVICE_NAME", "alias")
        monkeypatch.setenv("OBSERVABILITY_ENV", "dev")

        client = ObservabilityClient.from_env(profile="script")
        try:
            assert client.settings.service_name == "primary"
        finally:
            client.close()

    def test_explicit_arg_beats_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_SERVICE_NAME", "from-env")
        monkeypatch.setenv("OBSERVABILITY_ENV", "dev")

        client = ObservabilityClient.from_env(service_name="explicit", profile="script")
        try:
            assert client.settings.service_name == "explicit"
        finally:
            client.close()

    def test_enable_mlflow_env_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OBSERVABILITY_SERVICE_NAME", "svc")
        monkeypatch.setenv("OBSERVABILITY_ENV", "dev")
        monkeypatch.setenv("OBSERVABILITY_ENABLE_MLFLOW", "false")

        client = ObservabilityClient.from_env(profile="service", enable_tracing=False)
        try:
            # env-supplied False wins over the service-profile default True
            assert client.settings.enable_mlflow is False
        finally:
            client.close()


class TestParseBool:
    @pytest.mark.parametrize("token", ["1", "true", "TRUE", "yes", "on", "y"])
    def test_truthy_tokens(self, token: str) -> None:
        assert ObservabilityClient._parse_bool(token) is True

    @pytest.mark.parametrize("token", ["0", "false", "FALSE", "no", "off", "n"])
    def test_falsy_tokens(self, token: str) -> None:
        assert ObservabilityClient._parse_bool(token) is False

    def test_none_returns_none(self) -> None:
        assert ObservabilityClient._parse_bool(None) is None

    def test_empty_returns_none(self) -> None:
        assert ObservabilityClient._parse_bool("   ") is None

    def test_invalid_token_raises(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityClient._parse_bool("maybe")
