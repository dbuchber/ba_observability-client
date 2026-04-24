"""Example helper script using the reusable observability logger."""

from __future__ import annotations

import json
import random
import sys
import time
import uuid
from pathlib import Path

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from observability_client import ObservabilityClient


def main() -> None:
    """Run a small helper task and report to the observability stack."""
    obs = ObservabilityClient.full_mode(
        service_name="helper-script",
        experiment_name="rag-kg-thesis",
    )

    obs.push_log("helper_started", stage="startup")

    obs.start_run(
        run_name="helper-script-run",
        params={
            "dataset_name": "wiki_sample_v1",
            "dataset_hash": "sha256:example",
            "task": "maintenance-helper",
        },
        tags={"script_type": "helper"},
    )

    try:
        request_id = str(uuid.uuid4())
        query_hash = uuid.uuid5(uuid.NAMESPACE_DNS, request_id).hex[:12]

        with obs.span("sparql.query", request_id=request_id, query_hash=query_hash):
            t0 = time.perf_counter()
            time.sleep(random.uniform(0.05, 0.2))
            latency_ms = (time.perf_counter() - t0) * 1000

        obs.log_sparql(
            stage="sparql",
            query_hash=query_hash,
            latency_ms=round(latency_ms, 2),
            result_count=random.randint(1, 15),
            sparql_endpoint="http://fuseki:3030/ds/query",
            request_id=request_id,
            dataset_hash="sha256:example",
        )

        obs.log_metrics({"helper_latency_ms": latency_ms})

        summary_path = "helper_summary.json"
        with open(summary_path, "w", encoding="utf-8") as file:
            json.dump({"request_id": request_id, "latency_ms": latency_ms}, file, indent=2)
        obs.log_artifact(summary_path)

        obs.log_event("helper_completed", stage="complete", request_id=request_id)
        obs.push_log(
            "process_done",
            stage="complete",
            request_id=request_id,
            latency_ms=round(latency_ms, 2),
            query_hash=query_hash,
        )
    except Exception as exc:  # noqa: BLE001
        obs.log_event("helper_failed", level="error", error=str(exc))
        obs.push_log("helper_failed", level="error", error=str(exc))
        raise
    finally:
        obs.close()


if __name__ == "__main__":
    main()
