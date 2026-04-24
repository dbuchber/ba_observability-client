# ba-observability-client

Reusable Python observability client for structured logs, Loki push ingestion, OpenTelemetry tracing, and MLflow run tracking.

This package is designed for:

- **simple scripts** (quick logging with low setup),
- **long-running services** (request spans + propagated context),
- **experiment workloads** (MLflow params/metrics/artifacts + logs/traces).

## Features

- Structured JSON logging (`log_event`, `log_sparql`)
- Asynchronous Loki push delivery (`push_log`) with optional strict sync mode
- OpenTelemetry spans (`span`) and HTTP propagation helpers:
  - `fastapi_request_span` (inbound extraction)
  - `httpx_request` / `inject_trace_headers` (outbound injection)
- MLflow integration:
  - `start_run`, `log_params`, `log_metrics`, `log_artifact`, `close`
- Context binding for correlation (`bind`) across logs and traces
- Shared default-client convenience helpers:
  - `get_default_client`, `reset_default_client`
  - `log_event`, `log_info`, `log_warning`, `log_error`

## Requirements

- Python **3.12+**

Core dependencies (from `pyproject.toml`):

- `mlflow>=3.11.1`
- `opentelemetry-distro>=0.62b1`
- `opentelemetry-exporter-otlp>=1.41.1`

Some examples use additional packages (`fastapi`, `httpx`, `prometheus-client`, `uvicorn`).

## Installation

From this repository (editable install):

```bash
pip install -e .
```

Using `uv`:

```bash
uv pip install -e .
```

## Quick Start

### 1) Minimal script logging mode

```python
from observability_client import ObservabilityClient

client = ObservabilityClient.quick_script_mode(
    service_name="my-script",
    env="dev",
)

scoped = client.bind(task="daily-job")
scoped.log_event("job_started")
scoped.push_log("job_started")

scoped.log_event("job_finished", records=42)
client.close()
```

`quick_script_mode` is intentionally lightweight: MLflow and tracing are disabled by default.
If you want tracing in a script, construct `ObservabilityClient(...)` directly and set
`profile="script", enable_tracing=True`.

### 2) Full mode with MLflow + tracing

```python
from observability_client import ObservabilityClient

client = ObservabilityClient.full_mode(
    service_name="experiment-runner",
    env="dev",
    experiment_name="rag-kg-thesis",
)

run_id = client.start_run(params={"dataset": "wiki_sample_v1"})
client.log_event("run_started", run_id=run_id)
client.log_metrics({"latency_ms": 123.4})
client.close()
```

## Configuration

Configuration precedence is always:

> **explicit kwargs > environment variables > hardcoded defaults**

### Environment variables used by `ObservabilityClient.from_env`

| Purpose | Variables (in lookup order) | Default / Behavior |
|---|---|---|
| Service name | `OBSERVABILITY_SERVICE_NAME`, `SERVICE_NAME` | **Required** (raises if missing) |
| Environment | `OBSERVABILITY_ENV`, `ENV` | **Required** (raises if missing) |
| Profile | `OBSERVABILITY_PROFILE`, `TELEMETRY_PROFILE` | `service` |
| Experiment name | `EXPERIMENT_NAME`, `MLFLOW_EXPERIMENT_NAME` | `rag-kg-thesis` |
| MLflow tracking URI | `MLFLOW_TRACKING_URI` | `http://localhost:5000` |
| OTLP endpoint | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` |
| Loki push endpoint | `ALLOY_LOG_PUSH_ENDPOINT`, `LOKI_PUSH_ENDPOINT`, `LOKI_URL` | `http://localhost:9999/loki/api/v1/push` |
| Loki job label | `OBSERVABILITY_LOKI_JOB`, `LOKI_JOB` | `host-python` |
| Enable MLflow | `OBSERVABILITY_ENABLE_MLFLOW`, `ENABLE_MLFLOW` | Profile preset if unset |
| Enable tracing | `OBSERVABILITY_ENABLE_TRACING`, `ENABLE_TRACING` | Profile preset if unset |

### Profile presets

- `script`: MLflow disabled, tracing disabled
- `service`: MLflow enabled, tracing enabled
- `agent`: MLflow enabled, tracing enabled

You can still override either toggle explicitly.

## Usage Patterns

### Construct a client from env

```python
from observability_client import ObservabilityClient

client = ObservabilityClient.from_env(profile="service")
client.log_event("service_started")
client.close()
```

### Bind context for correlation

```python
scoped = client.bind(request_id="abc123", component="retriever")
scoped.log_event("retrieve_started")
scoped.push_log("retrieve_started")
```

Bound fields are merged into log payloads and reused by tracing helpers.

### Use package-level default helpers

```python
from observability_client import get_default_client, log_info

client = get_default_client(service_name="my-service", env="dev")
log_info("startup_complete", port=8000)
client.close()
```

## Tracing Integration

### FastAPI inbound + outbound `httpx` forwarding

```python
from fastapi import FastAPI, Request
import httpx

app = FastAPI()

@app.get("/proxy")
def proxy(request: Request):
    with client.fastapi_request_span(request=request, span_name="proxy.handle") as request_id:
        scoped = client.bind(request_id=request_id)
        with httpx.Client(timeout=3.0) as http_client:
            response = scoped.httpx_request(
                http_client.request,
                method="GET",
                url="http://127.0.0.1:8010/downstream",
            )
        return response.json()
```

## MLflow Integration

When MLflow is enabled:

1. `start_run(...)`
2. `log_params(...)`
3. `log_metrics(...)`
4. `log_artifact(path)`
5. `close()` to end the run

If MLflow is disabled (e.g., script profile), calling `start_run` raises a `RuntimeError`.

## Loki Push Logging Contract

Default endpoint:

- `http://localhost:9999/loki/api/v1/push`

`push_log(...)` behavior:

- **Default:** asynchronous enqueue (non-blocking)
- **Strict mode:** `raise_on_error=True` sends synchronously and raises on delivery failures

Label guidance:

- Keep labels low-cardinality (`job`, `service`, `env`, `level`)
- Put high-cardinality identifiers (`request_id`, UUIDs, query hashes) in JSON fields, not labels

## Example Scripts

Run examples from repository root:

```bash
python examples/minimal_script.py
python examples/helper_script.py
python examples/python_observability.py --mode batch
python examples/multi_agent_demo.py
```

FastAPI forwarding example:

```bash
uvicorn examples.fastapi_httpx_forwarding:app --host 127.0.0.1 --port 8010
```

## Public API Summary

Primary exports (`observability_client`):

- `ObservabilityClient`
- `TelemetryClient` (alias)
- `BoundObservabilityClient`
- `get_default_client(...)`
- `reset_default_client()`
- `log_event(...)`, `log_info(...)`, `log_warning(...)`, `log_error(...)`

## Troubleshooting

- **`ValueError` for missing `service_name` / `env`**
  - Set required env vars or pass values explicitly.
- **MLflow runtime errors**
  - Ensure MLflow is installed and enabled for your chosen profile.
- **No traces visible**
  - Verify OTEL dependencies and endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`).
- **No Loki logs arriving**
  - Check push endpoint path includes `/loki/api/v1/push` and that your receiver is reachable.
