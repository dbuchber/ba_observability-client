# ba-observability-client

Reusable Python observability client for structured logs, Loki push ingestion, OpenTelemetry tracing, and MLflow run tracking.

This package is designed for:

- **simple scripts** (quick logging with low setup),
- **long-running services** (request spans + propagated context),
- **experiment workloads** (MLflow params/metrics/artifacts + logs/traces).

## Features

- Structured JSON logging (`log_event`, `log_sparql`)
- Asynchronous Loki push delivery (`log_event(..., push=True)`) with optional strict sync mode
- OpenTelemetry tracing, exported via OTLP/HTTP (e.g. to Tempo):
  - Auto-instrumentation for outbound `httpx` and inbound FastAPI (default on for
    `service` / `agent`) — see `instrument_fastapi`
  - Manual span helpers: `span`, `fastapi_request_span` (inbound extraction),
    `httpx_request` / `inject_trace_headers` (outbound injection)
  - `trace_id` / `span_id` automatically added to every log event for log↔trace correlation
- MLflow integration:
  - `start_run`, `log_params`, `log_metrics`, `log_artifact`, `close`
- Context binding for correlation (`bind`) across logs and traces
- Shared default-client convenience helpers:
  - `get_default_client`, `reset_default_client`
  - `log_event`, `log_info`, `log_warning`, `log_error`

## Requirements

- Python **3.12+**

Core dependencies (from `pyproject.toml`):

- `httpx>=0.28.1`
- `mlflow>=3.11.1`
- `opentelemetry-distro>=0.62b1`
- `opentelemetry-exporter-otlp>=1.41.1`
- `opentelemetry-instrumentation-httpx==0.62b1`
- `opentelemetry-instrumentation-fastapi==0.62b1`

The two `opentelemetry-instrumentation-*` packages are pinned to match the
distro version and power the auto-instrumentation described under
[Tracing Integration](#tracing-integration). Some examples use additional
packages (`fastapi`, `prometheus-client`, `uvicorn`).

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
scoped.log_event("job_started", push=True)

scoped.log_event("job_finished", records=42, push=True)
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
| Enable auto-instrumentation | `OBSERVABILITY_AUTO_INSTRUMENTATION`, `ENABLE_AUTO_INSTRUMENTATION` | Follows resolved tracing if unset |

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
scoped.log_event("retrieve_started", push=True)
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

Traces are exported via OTLP/HTTP to `{otlp_endpoint}/v1/traces` (default
`http://localhost:4318`, i.e. a Tempo/OTLP-collector HTTP receiver on port
**4318** — not the gRPC port 4317). Every log event also carries `trace_id` /
`span_id` of the active span, so logs and traces correlate in Grafana via a
`trace_id` derived field (Loki → Tempo).

### Auto-instrumentation (recommended for services)

For `service` / `agent` profiles, OpenTelemetry auto-instrumentation is enabled
by default (`enable_auto_instrumentation`, follows `enable_tracing`). This means:

- **Outbound `httpx`** is instrumented globally on construction — every request
  gets a CLIENT span and W3C `traceparent` propagation, with or without the
  `httpx_request` helper.
- **Inbound FastAPI** is instrumented per-app — call `client.instrument_fastapi(app)`
  once after creating the app:

  ```python
  app = FastAPI()
  client.instrument_fastapi(app)   # installs the OTel SERVER-span middleware
  ```

When auto-instrumentation is active, the manual helpers
(`httpx_request`, `fastapi_request_span`) **stop creating their own spans** to
avoid duplicates — they only forward / bind the business `request_id` onto the
instrumentor's span. So you keep the `request_id` ↔ log coupling while the
instrumentor owns span creation and propagation.

Disable per client with `enable_auto_instrumentation=False` (or env
`OBSERVABILITY_AUTO_INSTRUMENTATION=0`) to fall back to manual-only spans.

> Requires `opentelemetry-instrumentation-httpx` / `-fastapi` (declared deps). If
> missing, the client logs `auto_instrumentation_dependency_missing` and the
> manual helpers create the spans instead — it never raises.

### Distributed setup (traces across nodes)

A single Tempo collects spans from all nodes; traces join via the W3C
`traceparent` header that propagates over inter-service HTTP calls. Point every
node at the same Tempo:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://<tempo-host>:4318
```

If a node cannot reach Tempo directly (e.g. behind NAT/SSH), forward the OTLP
port through your tunnel and point the node at `http://localhost:4318`.

### FastAPI inbound + outbound `httpx` forwarding

```python
from fastapi import FastAPI, Request
import httpx

app = FastAPI()
client.instrument_fastapi(app)   # SERVER spans created automatically (auto-instrumentation)

@app.get("/proxy")
def proxy(request: Request):
    # With auto-instrumentation on, fastapi_request_span / httpx_request do NOT
    # create their own spans — they only resolve/forward request_id and enrich
    # the instrumentor's span. The pattern below stays the same either way.
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

If you disable auto-instrumentation (`enable_auto_instrumentation=False`), the
exact same code path keeps working — the helpers then create the spans
themselves.

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

Push behavior (`log_event(..., push=True)`):

- **Default:** asynchronous enqueue (non-blocking). A daemon worker drains a
  bounded queue (capacity 2000) and POSTs to the Loki push endpoint.
- **Strict mode:** `raise_on_error=True` sends synchronously and raises on
  delivery failures.
- **After `close()`:** the worker is stopped and subsequent
  `log_event(..., push=True)` calls fall back to synchronous delivery so
  shutdown-time events still ship instead of vanishing into the stopped queue.

Diagnostic events emitted by the client itself (throttled to ~30s intervals so
chronic problems stay visible without flooding stdout):

- `loki_queue_full_dropping_events` — the async queue is full and events are
  being dropped. Includes the running drop count.
- `loki_delivery_failed` — the worker tried to deliver and the POST failed
  (Loki unreachable, transport error, etc.). Includes the running failure
  count.
- `loki_push_endpoint_missing` — emitted once if no endpoint is configured.
- `tracer_provider_already_installed` — another OpenTelemetry `TracerProvider`
  was already installed (by another client, by `opentelemetry-distro`
  auto-init, etc.); the client reuses it instead of clobbering the global.
- `tracing_dependency_missing` — `enable_tracing=True` was requested but the
  OpenTelemetry packages are not installed; structured logging continues
  unaffected.

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

Key `ObservabilityClient` methods:

- Logging: `log_event`, `log_sparql`, `bind`
- Tracing: `span`, `fastapi_request_span`, `httpx_request`, `inject_trace_headers`,
  `instrument_fastapi` (activate FastAPI auto-instrumentation on an app)
- MLflow: `start_run`, `log_params`, `log_metrics`, `log_artifact`
- Construction: `from_env`, `quick_script_mode`, `full_mode`
- Lifecycle: `close`

## Troubleshooting

- **`ValueError` for missing `service_name` / `env`**
  - Set required env vars or pass values explicitly.
- **MLflow runtime errors**
  - Ensure MLflow is installed and enabled for your chosen profile.
- **No traces visible**
  - Verify OTEL dependencies and endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`); the
    endpoint must point at an OTLP **HTTP** receiver (port 4318, not 4317).
  - For inbound FastAPI spans with auto-instrumentation, ensure you called
    `client.instrument_fastapi(app)`.
- **Duplicate spans for the same request**
  - You are creating manual spans on top of auto-instrumentation. The helpers
    suppress this automatically; if you see duplicates, check that you are not
    running both `opentelemetry-instrument` (the launcher) *and* this client's
    auto-instrumentation.
- **No Loki logs arriving**
  - Check push endpoint path includes `/loki/api/v1/push` and that your receiver is reachable.
