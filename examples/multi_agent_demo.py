"""Two-agent demo with Agent-A orchestrating Agent-B calculator tool calls.

This script demonstrates:

- Agent-A coordinating work,
- Agent-B handling delegated tasks,
- a mock calculator tool (`add`, `subtract`, `multiply`, `divide`),
- structured logs, Loki push events, and trace spans via ``ObservabilityClient``,
- optional MLflow distributed trace-context propagation across Agent-A -> Agent-B.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import sys
import uuid
from pathlib import Path

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from observability_client import ObservabilityClient

try:
    import mlflow as _mlflow
except ImportError:  # pragma: no cover
    _mlflow = None


def _mlflow_trace(func):
    """Decorate ``func`` with ``mlflow.trace`` when available.

    Args:
        func (Any): Function to decorate.

    Returns:
        Any: Traced function when supported, otherwise original function.
    """
    if _mlflow is None or not hasattr(_mlflow, "trace"):
        return func
    return _mlflow.trace(func)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse a boolean token from environment-style strings.

    Args:
        value (str | None): Raw value from environment.
        default (bool, optional): Fallback when unset or empty.

    Returns:
        bool: Parsed value.
    """
    if value is None:
        return default

    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "on", "y"}


def _configure_optional_mlflow_autotracing() -> bool:
    """Configure optional MLflow GenAI tracing.

    This follows MLflow distributed tracing guidance while staying optional for
    environments that do not use MLflow.

    Returns:
        bool: ``True`` when MLflow instrumentation is enabled.
    """
    enable_mlflow = _parse_bool(os.getenv("DEMO_ENABLE_MLFLOW"), default=False)
    if not enable_mlflow:
        return False

    if _mlflow is None:
        return False

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "multi-agent-demo")
    _mlflow.set_tracking_uri(tracking_uri)
    _mlflow.set_experiment(experiment_name)

    return True


def _get_mlflow_tracing_headers() -> dict[str, str]:
    """Return MLflow distributed tracing headers for outbound calls.

    Returns:
        dict[str, str]: HTTP-style tracing headers (for example ``traceparent``).
    """
    if _mlflow is None:
        return {}

    tracing_module = getattr(_mlflow, "tracing", None)
    if tracing_module is None:
        return {}

    getter = getattr(tracing_module, "get_tracing_context_headers_for_http_request", None)
    if not callable(getter):
        return {}

    raw_headers = getter()
    if not isinstance(raw_headers, dict):
        return {}

    return {str(key): str(value) for key, value in raw_headers.items()}


@contextlib.contextmanager
def _set_mlflow_remote_context(headers: dict[str, str]):
    """Set MLflow tracing context from inbound HTTP-style headers.

    Args:
        headers (dict[str, str]): Incoming request headers.

    Yields:
        None: Context where extracted trace context is active.
    """
    if _mlflow is None:
        yield
        return

    tracing_module = getattr(_mlflow, "tracing", None)
    if tracing_module is None:
        yield
        return

    setter = getattr(tracing_module, "set_tracing_context_from_http_request_headers", None)
    if not callable(setter):
        yield
        return

    with setter(headers):
        yield


@_mlflow_trace
def calculator_tool(operation: str, left: float, right: float) -> float:
    """Execute a simple calculator operation.

    Args:
        operation (str): Calculator operation (`add`, `subtract`, `multiply`, `divide`).
        left (float): Left operand.
        right (float): Right operand.

    Returns:
        float: Computation result.

    Raises:
        ValueError: If operation is unsupported.
        ZeroDivisionError: If dividing by zero.
    """
    if operation == "add":
        return left + right
    if operation == "subtract":
        return left - right
    if operation == "multiply":
        return left * right
    if operation == "divide":
        if right == 0:
            raise ZeroDivisionError("Cannot divide by zero.")
        return left / right
    raise ValueError(f"Unsupported calculator operation: {operation}")


class AgentB:
    """Agent-B executes delegated calculator tasks using a mock tool."""

    def __init__(self, client: ObservabilityClient) -> None:
        """Initialize Agent-B.

        Args:
            client (ObservabilityClient): Shared observability client.
        """
        self._client = client.bind(agent="agent-b")

    @_mlflow_trace
    def solve(self, *, request_id: str, operation: str, left: float, right: float) -> float:
        """Solve one delegated arithmetic task.

        Args:
            request_id (str): End-to-end request identifier.
            operation (str): Calculator operation.
            left (float): Left operand.
            right (float): Right operand.

        Returns:
            float: Calculator result.
        """
        scoped = self._client.bind(
            request_id=request_id,
            operation=operation,
            tool_name="mock-calculator",
        )
        with scoped.span("agent_b.solve", operation=operation):
            scoped.log_event("agent_b_received_task", left=left, right=right)
            with scoped.span("tool.calculator.execute"):
                result = calculator_tool(operation=operation, left=left, right=right)
            scoped.log_event("agent_b_completed_task", result=result, push=True)
            return result

    @_mlflow_trace
    def handle_remote_call(
        self,
        *,
        inbound_headers: dict[str, str],
        request_id: str,
        operation: str,
        left: float,
        right: float,
    ) -> float:
        """Handle a simulated remote call from Agent-A.

        Args:
            inbound_headers (dict[str, str]): Tracing headers from Agent-A.
            request_id (str): End-to-end request identifier.
            operation (str): Calculator operation.
            left (float): Left operand.
            right (float): Right operand.

        Returns:
            float: Calculator result from delegated solve step.
        """
        scoped = self._client.bind(request_id=request_id, transport="simulated-http")
        with _set_mlflow_remote_context(inbound_headers):
            with scoped.span(
                "agent_b.remote_handler",
                propagated_traceparent=inbound_headers.get("traceparent"),
            ):
                scoped.log_event(
                    "agent_b_remote_call_received",
                    has_traceparent="traceparent" in inbound_headers,
                )
                return self.solve(
                    request_id=request_id,
                    operation=operation,
                    left=left,
                    right=right,
                )


class AgentA:
    """Agent-A orchestrates the workflow and delegates arithmetic to Agent-B."""

    def __init__(self, client: ObservabilityClient, agent_b: AgentB) -> None:
        """Initialize Agent-A.

        Args:
            client (ObservabilityClient): Shared observability client.
            agent_b (AgentB): Delegated worker agent.
        """
        self._client = client.bind(agent="agent-a")
        self._agent_b = agent_b

    @_mlflow_trace
    def run(self) -> dict[str, float | str]:
        """Run one orchestration flow using Agent-B tool calls.

        Returns:
            dict[str, float | str]: Final summary payload.
        """
        request_id = uuid.uuid4().hex
        scoped = self._client.bind(request_id=request_id, workflow="multi_agent_demo")

        with scoped.span("agent_a.orchestrate"):
            scoped.log_event("agent_a_started")

            with scoped.span("agent_a.call_agent_b", operation="multiply"):
                headers_for_b = _get_mlflow_tracing_headers()
                scoped.log_event(
                    "agent_a_forwarding_to_agent_b",
                    has_traceparent="traceparent" in headers_for_b,
                    operation="multiply",
                )
                intermediate = self._agent_b.handle_remote_call(
                    inbound_headers=headers_for_b,
                    request_id=request_id,
                    operation="multiply",
                    left=6,
                    right=7,
                )

            with scoped.span("agent_a.call_agent_b", operation="add"):
                headers_for_b = _get_mlflow_tracing_headers()
                scoped.log_event(
                    "agent_a_forwarding_to_agent_b",
                    has_traceparent="traceparent" in headers_for_b,
                    operation="add",
                )
                final = self._agent_b.handle_remote_call(
                    inbound_headers=headers_for_b,
                    request_id=request_id,
                    operation="add",
                    left=intermediate,
                    right=5,
                )

            scoped.log_event(
                "agent_a_completed",
                intermediate_result=intermediate,
                final_result=final,
                push=True,
            )
            return {
                "request_id": request_id,
                "intermediate_result": intermediate,
                "final_result": final,
            }


def main() -> None:
    """Run the two-agent demo and print the final payload."""
    mlflow_enabled = _configure_optional_mlflow_autotracing()
    client = ObservabilityClient(
        service_name="multi-agent-demo",
        env=os.getenv("OBSERVABILITY_ENV", "dev"),
        profile="agent",
        enable_mlflow=False,
        enable_tracing=True,
    )
    try:
        client.log_event(
            "multi_agent_demo_started",
            mlflow_enabled=mlflow_enabled,
            mlflow_installed=importlib.util.find_spec("mlflow") is not None,
            distributed_tracing_mode="header_propagation",
        )
        agent_b = AgentB(client)
        agent_a = AgentA(client, agent_b)
        result = agent_a.run()
        print(json.dumps(result, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
