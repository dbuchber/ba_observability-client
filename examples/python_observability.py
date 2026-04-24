"""Example Python workload wired into MLflow, metrics, logs, and traces.

This module demonstrates both batch and API-like execution patterns for
experiment services running in Docker Compose, using the ObservabilityClient.
"""

from __future__ import annotations

import argparse
import os
import random
import time
import uuid
import sys
from pathlib import Path

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from prometheus_client import Counter, Histogram, start_http_server
from observability_client import ObservabilityClient

SPARQL_QUERY_DURATION_SECONDS = Histogram(
    "sparql_query_duration_seconds",
    "Duration of SPARQL queries.",
    ["endpoint", "query_hash"],
)
SPARQL_QUERY_ERRORS_TOTAL = Counter(
    "sparql_query_errors_total",
    "Total number of SPARQL query failures.",
    ["endpoint"],
)
SPARQL_RESULTS_TOTAL = Counter(
    "sparql_results_total",
    "Total number of SPARQL results processed.",
    ["endpoint"],
)
KG_TRAVERSAL_STEPS_TOTAL = Counter(
    "kg_traversal_steps_total",
    "Total number of traversal steps.",
)
KG_NODES_VISITED_TOTAL = Counter(
    "kg_nodes_visited_total",
    "Total number of visited KG nodes.",
)


def run_single_request(
    *,
    client: ObservabilityClient,
    sparql_endpoint: str,
) -> None:
    """Simulate one RDF/SPARQL request with logs, metrics, and traces.

    Args:
        client (ObservabilityClient): Observability client.
        sparql_endpoint (str): Endpoint to simulate.
    """
    request_id = str(uuid.uuid4())
    query_hash = uuid.uuid5(uuid.NAMESPACE_DNS, request_id).hex[:12]

    with client.span("sparql.query", endpoint=sparql_endpoint, query_hash=query_hash):
        t0 = time.perf_counter()
        simulated_latency = random.uniform(0.05, 0.5)
        time.sleep(simulated_latency)
        latency = time.perf_counter() - t0
        result_count = random.randint(1, 25)

        SPARQL_QUERY_DURATION_SECONDS.labels(
            endpoint=sparql_endpoint,
            query_hash=query_hash,
        ).observe(latency)
        SPARQL_RESULTS_TOTAL.labels(endpoint=sparql_endpoint).inc(result_count)

    with client.span("kg.traverse") as span:
        steps = random.randint(2, 8)
        nodes = random.randint(5, 30)
        KG_TRAVERSAL_STEPS_TOTAL.inc(steps)
        KG_NODES_VISITED_TOTAL.inc(nodes)
        if span:
            span.set_attribute("depth", steps)
            span.set_attribute("nodes_visited", nodes)

    # Use specialized SPARQL logging helper
    client.log_sparql(
        query_hash=query_hash,
        stage="sparql",
        latency_ms=round(latency * 1000, 2),
        result_count=result_count,
        sparql_endpoint=sparql_endpoint,
        request_id=request_id,
        http_status=200,
    )

    client.log_metrics(
        {
            "last_request_latency_ms": latency * 1000,
            "last_result_count": result_count,
        }
    )


def run_batch(client: ObservabilityClient, sparql_endpoint: str) -> None:
    """Run one-shot batch experiment simulation.

    Args:
        client (ObservabilityClient): Observability client.
        sparql_endpoint (str): Endpoint to simulate.
    """
    for _ in range(5):
        run_single_request(client=client, sparql_endpoint=sparql_endpoint)

    summary = {"mode": "batch", "status": "ok", "run_id": client.run_id}
    import json
    with open("batch_results.json", "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)
    client.log_artifact("batch_results.json")


def run_api(client: ObservabilityClient, sparql_endpoint: str) -> None:
    """Run long-lived API-like loop for repeated requests.

    Args:
        client (ObservabilityClient): Observability client.
        sparql_endpoint (str): Endpoint to simulate.
    """
    client.log_event("api_started", stage="startup")
    while True:
        try:
            run_single_request(client=client, sparql_endpoint=sparql_endpoint)
            time.sleep(1.0)
        except Exception:
            SPARQL_QUERY_ERRORS_TOTAL.labels(endpoint=sparql_endpoint).inc()
            client.log_event("request_failed", level="error", stage="error")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description="Observability reference workload")
    parser.add_argument(
        "--mode",
        choices=["batch", "api"],
        default="batch",
        help="Execution style for this demo workload.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the observability demo workload."""
    args = parse_args()

    service_name = os.getenv("SERVICE_NAME", "experiment-runner")
    env = os.getenv("ENV", "dev")
    sparql_endpoint = os.getenv("SPARQL_ENDPOINT", "http://fuseki:3030/ds/query")
    
    # Initialize the production-ready client
    client = ObservabilityClient.from_env(
        service_name=service_name,
        env=env,
        profile="service"
    )

    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)

    # Start MLflow run via client
    client.start_run(
        params={
            "dataset_name": os.getenv("DATASET_NAME", "wiki_sample_v1"),
            "dataset_hash": os.getenv("DATASET_HASH", "sha256:example"),
            "approach": os.getenv("APPROACH", "hybrid"),
        }
    )

    try:
        if args.mode == "batch":
            run_batch(client=client, sparql_endpoint=sparql_endpoint)
        else:
            run_api(client=client, sparql_endpoint=sparql_endpoint)
    finally:
        client.close()


if __name__ == "__main__":
    main()
