"""Minimal one-off script example for observability logging and traces.

This example is intentionally lightweight and does not require MLflow.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from observability_client import ObservabilityClient


def main() -> None:
    """Run a minimal task with structured logs and one trace span."""
    client = ObservabilityClient(
        service_name="minimal-script",
        env=os.getenv("OBSERVABILITY_ENV", "dev"),
        profile="script",
        enable_mlflow=False,
        enable_tracing=True,
    )

    task_client = client.bind(task="minimal_demo")
    task_client.log_event("minimal_script_started", push=True)

    with task_client.span("minimal.work"):
        started = time.perf_counter()
        time.sleep(0.05)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)

    task_client.log_event("minimal_script_completed", elapsed_ms=elapsed_ms, push=True)
    client.close()


if __name__ == "__main__":
    main()
